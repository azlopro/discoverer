package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type wcConfig struct {
	ServerURL     string
	Token         string
	WorkerID      string
	BatchSize     int
	BatchInterval time.Duration
	SpoolDir      string
	HTTPTimeout   time.Duration
	UserAgent     string
	// Compress gzips the /results POST body and sets
	// Content-Encoding: gzip. The server transparently decodes it.
	Compress bool
	// SpoolReplayInterval re-tries the spool dir on a cadence so a
	// transient network outage doesn't strand results until the next
	// process restart. 0 disables (startup-only replay).
	SpoolReplayInterval time.Duration
}

// chunkedResult pairs a probe outcome with the chunk_id that owns it.
// chunk_id sticks with the result through batching/spooling so the
// server-side ingest writes the right scan_results.chunk_id even when
// /chunk-complete races ahead of an in-flight /results POST.
type chunkedResult struct {
	res     scanResult
	chunkID int64
}

// wcClient batches scan results and ships them to the server.
type wcClient struct {
	cfg    wcConfig
	hc     *http.Client
	in     chan chunkedResult
	closed chan struct{}
	fatal  chan error
	once   sync.Once

	// flushReq lets callers force-drain the in-flight buffer and wait
	// until every queued result has been POSTed (or spooled). Used by
	// runChunk to make /chunk-complete strictly-after the chunk's
	// /results POSTs.
	flushReq chan chan struct{}
}

func newWC(cfg wcConfig) (*wcClient, error) {
	if cfg.ServerURL == "" {
		return nil, errors.New("server URL required")
	}
	if cfg.Token == "" {
		return nil, errors.New("api token required")
	}
	if cfg.WorkerID == "" {
		return nil, errors.New("worker id required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	if cfg.BatchInterval <= 0 {
		cfg.BatchInterval = 5 * time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 30 * time.Second
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "scrap-metal-worker/1"
	}
	if cfg.SpoolDir != "" {
		if err := os.MkdirAll(cfg.SpoolDir, 0o755); err != nil {
			return nil, fmt.Errorf("spool dir: %w", err)
		}
	}
	cfg.ServerURL = strings.TrimRight(cfg.ServerURL, "/")

	c := &wcClient{
		cfg:      cfg,
		hc:       &http.Client{},
		in:       make(chan chunkedResult, cfg.BatchSize*4),
		closed:   make(chan struct{}),
		fatal:    make(chan error, 1),
		flushReq: make(chan chan struct{}, 1),
	}
	go c.run()
	return c, nil
}

// FatalCh fires once with a non-recoverable error (auth rejected). Main
// should drain the worker, Close(), and exit.
func (c *wcClient) FatalCh() <-chan error { return c.fatal }

// SubmitWithChunk queues a result tagged with the chunk that produced
// it. Blocks if the buffer is full.
func (c *wcClient) SubmitWithChunk(r scanResult, chunkID int64) {
	defer func() {
		_ = recover()
	}()
	c.in <- chunkedResult{res: r, chunkID: chunkID}
}

// FlushAndWait forces a flush of the in-flight buffer and waits for it
// to complete (or ctx to fire). Use before /chunk-complete to ensure
// the chunk's results are durably accepted before the chunk is acked.
func (c *wcClient) FlushAndWait(ctx context.Context) {
	done := make(chan struct{})
	select {
	case c.flushReq <- done:
	case <-ctx.Done():
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Close flushes the in-flight buffer (best-effort; spools on failure)
// and waits for the worker loop to exit.
func (c *wcClient) Close() {
	c.once.Do(func() { close(c.in) })
	<-c.closed
}

func (c *wcClient) run() {
	defer close(c.closed)

	// Group results by chunk_id so each batch carries exactly one
	// chunk_id over the wire (api.IngestRequest.ChunkID).
	bufByChunk := make(map[int64][]scanResult)
	totalBuffered := 0
	tick := time.NewTicker(c.cfg.BatchInterval)
	defer tick.Stop()

	var replayTickC <-chan time.Time
	if c.cfg.SpoolReplayInterval > 0 {
		rt := time.NewTicker(c.cfg.SpoolReplayInterval)
		defer rt.Stop()
		replayTickC = rt.C
	}
	var replayInFlight atomic.Bool

	// Kick startup replay async so a long backlog (e.g. after the
	// server was down all night) doesn't block submissions from the
	// scanner the moment it starts pulling chunks.
	if replayInFlight.CompareAndSwap(false, true) {
		go func() {
			defer replayInFlight.Store(false)
			c.replaySpool()
		}()
	}

	flush := func() {
		for cid, rows := range bufByChunk {
			if len(rows) == 0 {
				continue
			}
			if err := c.send(rows, cid); err != nil {
				log.Printf("worker: send %d rows (chunk %d) failed: %v — spooling", len(rows), cid, err)
				if serr := c.spool(rows, cid); serr != nil {
					log.Printf("worker: spool failed: %v (DATA LOSS)", serr)
				}
			}
			delete(bufByChunk, cid)
		}
		totalBuffered = 0
	}

	for {
		select {
		case r, ok := <-c.in:
			if !ok {
				flush()
				return
			}
			bufByChunk[r.chunkID] = append(bufByChunk[r.chunkID], r.res)
			totalBuffered++
			if totalBuffered >= c.cfg.BatchSize {
				flush()
			}
		case <-tick.C:
			flush()
		case <-replayTickC:
			// Run replay off the run-loop so a slow re-POST doesn't
			// block normal flushes. Drop the tick if a replay is
			// already in flight.
			if replayInFlight.CompareAndSwap(false, true) {
				go func() {
					defer replayInFlight.Store(false)
					c.replaySpool()
				}()
			}
		case done := <-c.flushReq:
			flush()
			close(done)
		}
	}
}

func (c *wcClient) send(buf []scanResult, chunkID int64) error {
	req := ingestRequest{
		WorkerID: c.cfg.WorkerID,
		ChunkID:  chunkID,
		Batch:    make([]resultRecord, len(buf)),
	}
	for i, r := range buf {
		req.Batch[i] = toRecord(r)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	const maxAttempts = 5
	var lastErr error
	backoff := time.Second
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := c.postOnce(pathResults, body)
		if err == nil {
			return nil
		}
		var fatal *fatalAuthError
		if errors.As(err, &fatal) {
			select {
			case c.fatal <- fatal:
			default:
			}
			return err
		}
		lastErr = err
		log.Printf("worker: attempt %d/%d: %v", attempt, maxAttempts, err)
		if attempt < maxAttempts {
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	return lastErr
}

type fatalAuthError struct{ msg string }

func (e *fatalAuthError) Error() string { return e.msg }

func (c *wcClient) postOnce(path string, body []byte) error {
	var (
		reader   io.Reader = bytes.NewReader(body)
		encoding string
	)
	// Compress only the bulky path (results). /heartbeat, /claim,
	// /chunk-complete are tiny and not worth the gzip overhead.
	if c.cfg.Compress && path == pathResults {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("gzip close: %w", err)
		}
		reader = &buf
		encoding = "gzip"
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.ServerURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("User-Agent", c.cfg.UserAgent)

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &fatalAuthError{msg: "401 from server: " + strings.TrimSpace(string(msg))}
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	if resp.StatusCode >= 400 {
		// 4xx other than 401 — permanent reject. Log and drop so we
		// don't loop forever on poison data.
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		log.Printf("worker: server rejected %s (http %d): %s — dropping",
			path, resp.StatusCode, strings.TrimSpace(string(msg)))
		return nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// Claim asks the server for the next pending chunk. An ok=Empty
// response means no work; the caller should sleep and retry.
func (c *wcClient) Claim(ctx context.Context) (claimResponse, error) {
	body, err := json.Marshal(claimRequest{WorkerID: c.cfg.WorkerID})
	if err != nil {
		return claimResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.ServerURL+pathClaim, bytes.NewReader(body))
	if err != nil {
		return claimResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("User-Agent", c.cfg.UserAgent)

	resp, err := c.hc.Do(req)
	if err != nil {
		return claimResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		fatal := &fatalAuthError{msg: "401 from server: " + strings.TrimSpace(string(msg))}
		select {
		case c.fatal <- fatal:
		default:
		}
		return claimResponse{}, fatal
	}
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return claimResponse{}, fmt.Errorf("claim http %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	var out claimResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return claimResponse{}, fmt.Errorf("claim decode: %w", err)
	}
	return out, nil
}

// ChunkComplete acknowledges a chunk. Pass non-empty errMsg to mark it
// as a soft failure (server will requeue subject to attempt cap).
//
// Acks are durable: if the server is unreachable after a few retries,
// the request body is spooled to disk and replayed by replaySpool() on
// the next opportunity. This is what keeps a chunk from being
// re-scanned by another worker just because the server happened to be
// down at the moment we finished it.
func (c *wcClient) ChunkComplete(ctx context.Context, chunkID int64, submitted int, errMsg string) error {
	body, err := json.Marshal(chunkCompleteRequest{
		WorkerID:  c.cfg.WorkerID,
		ChunkID:   chunkID,
		Submitted: submitted,
		Error:     errMsg,
	})
	if err != nil {
		return err
	}

	const maxAttempts = 3
	var lastErr error
	backoff := time.Second
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := c.postOnce(pathChunkComplete, body)
		if err == nil {
			return nil
		}
		var fatal *fatalAuthError
		if errors.As(err, &fatal) {
			select {
			case c.fatal <- fatal:
			default:
			}
			return err
		}
		lastErr = err
		if attempt < maxAttempts {
			time.Sleep(backoff)
			backoff *= 2
		}
	}

	if serr := c.spoolAck(body); serr != nil {
		return fmt.Errorf("ack post: %v; spool: %w", lastErr, serr)
	}
	log.Printf("worker: chunk-complete %d failed (%v) — spooled for retry", chunkID, lastErr)
	return nil
}

// Heartbeat does a no-payload ping. Useful to keep the worker visible
// in workers.last_seen_at when nothing's being submitted.
func (c *wcClient) Heartbeat(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.ServerURL+pathHeartbeat, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("heartbeat http %d", resp.StatusCode)
	}
	return nil
}

func (c *wcClient) spool(buf []scanResult, chunkID int64) error {
	if c.cfg.SpoolDir == "" {
		return errors.New("no spool dir configured")
	}
	req := ingestRequest{
		WorkerID: c.cfg.WorkerID,
		ChunkID:  chunkID,
		Batch:    make([]resultRecord, len(buf)),
	}
	for i, r := range buf {
		req.Batch[i] = toRecord(r)
	}
	name := filepath.Join(c.cfg.SpoolDir,
		fmt.Sprintf("spool-%d.json", time.Now().UnixNano()))
	tmp := name + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(req); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, name)
}

// spoolAck persists a chunk-complete request body to disk so a later
// replay can deliver it. Same atomic write-then-rename pattern as
// spool() to avoid partial files if the worker is killed mid-write.
func (c *wcClient) spoolAck(body []byte) error {
	if c.cfg.SpoolDir == "" {
		return errors.New("no spool dir configured")
	}
	name := filepath.Join(c.cfg.SpoolDir,
		fmt.Sprintf("ack-%d.json", time.Now().UnixNano()))
	tmp := name + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(body); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, name)
}

func (c *wcClient) replaySpool() {
	if c.cfg.SpoolDir == "" {
		return
	}
	entries, err := os.ReadDir(c.cfg.SpoolDir)
	if err != nil {
		log.Printf("worker: spool read: %v", err)
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		// Dispatch by filename prefix:
		//   spool-* → results batches      → POST /api/v1/results
		//   ack-*   → chunk-complete acks  → POST /api/v1/chunk-complete
		var apiPath string
		switch {
		case strings.HasPrefix(e.Name(), "spool-"):
			apiPath = pathResults
		case strings.HasPrefix(e.Name(), "ack-"):
			apiPath = pathChunkComplete
		default:
			continue
		}
		path := filepath.Join(c.cfg.SpoolDir, e.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			log.Printf("worker: read spool %s: %v", path, err)
			continue
		}
		if err := c.postRaw(apiPath, body); err != nil {
			log.Printf("worker: replay %s: %v (keeping for next boot)", path, err)
			continue
		}
		_ = os.Remove(path)
		log.Printf("worker: replayed spool %s", path)
	}
}

func (c *wcClient) postRaw(path string, body []byte) error {
	const maxAttempts = 3
	backoff := time.Second
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		err := c.postOnce(path, body)
		if err == nil {
			return nil
		}
		var fatal *fatalAuthError
		if errors.As(err, &fatal) {
			select {
			case c.fatal <- fatal:
			default:
			}
			return err
		}
		lastErr = err
		time.Sleep(backoff)
		backoff *= 2
	}
	return lastErr
}

func toRecord(r scanResult) resultRecord {
	return resultRecord{
		IP:          r.IP.String(),
		Port:        r.Port,
		ScannedAt:   r.ScannedAt,
		PortOpen:    r.PortOpen,
		HTTPStatus:  r.HTTPStatus,
		Headers:     r.Headers,
		Body:        r.Body,
		BodySize:    r.BodySize,
		ContentType: r.ContentType,
		Server:      r.Server,
		Title:       r.Title,
		Error:       r.Error,
		DurationMs:  r.DurationMs,
	}
}
