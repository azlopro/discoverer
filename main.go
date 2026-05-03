// Worker is a thin scrap-metal agent. It logs in to the central server
// with a bearer token, polls /api/v1/claim for the next chunk of (ip,
// port) pairs, runs the scan with the per-chunk options the server
// returned, ships results back via /api/v1/results, then marks the
// chunk done. Deploy by cloning the worker/ folder and setting three
// env vars (SERVER_URL, WORKER_API_KEY, optional WORKER_ID).
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/netip"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	cfg, err := loadConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		log.Fatalf("config: %v", err)
	}
	log.Printf("scrap-metal worker %s starting: server=%s idle-poll=%s compress=%t spool-replay=%s",
		cfg.WorkerID, cfg.ServerURL, cfg.IdlePoll, cfg.Compress, cfg.SpoolReplayInterval)

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		log.Printf("signal %s received, shutting down", s)
		cancel()
	}()
	defer cancel()

	wc, err := newWC(wcConfig{
		ServerURL:           cfg.ServerURL,
		Token:               cfg.APIKey,
		WorkerID:            cfg.WorkerID,
		BatchSize:           cfg.BatchSize,
		BatchInterval:       cfg.BatchInterval,
		SpoolDir:            cfg.SpoolDir,
		Compress:            cfg.Compress,
		SpoolReplayInterval: cfg.SpoolReplayInterval,
	})
	if err != nil {
		log.Fatalf("workerclient: %v", err)
	}

	go func() {
		err := <-wc.FatalCh()
		log.Printf("FATAL auth error from server: %v — shutting down", err)
		cancel()
	}()

	heartbeatStop := make(chan struct{})
	if cfg.HeartbeatEvery > 0 {
		go func() {
			t := time.NewTicker(cfg.HeartbeatEvery)
			defer t.Stop()
			for {
				select {
				case <-heartbeatStop:
					return
				case <-t.C:
					hbCtx, hbCancel := context.WithTimeout(context.Background(), 10*time.Second)
					if err := wc.Heartbeat(hbCtx); err != nil {
						log.Printf("heartbeat: %v", err)
					}
					hbCancel()
				}
			}
		}()
	}

	for ctx.Err() == nil {
		claimCtx, claimCancel := context.WithTimeout(ctx, 30*time.Second)
		chunk, err := wc.Claim(claimCtx)
		claimCancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			log.Printf("claim: %v — sleeping %s", err, cfg.IdlePoll)
			sleep(ctx, cfg.IdlePoll)
			continue
		}
		if chunk.Empty {
			sleep(ctx, cfg.IdlePoll)
			continue
		}
		log.Printf("claim: chunk=%d job=%d pairs=%d (lease until %s)",
			chunk.ChunkID, chunk.JobID, len(chunk.Pairs), chunk.LeaseExpiresAt.Format(time.RFC3339))

		submitted, runErr := runChunk(ctx, wc, chunk, cfg.JobBuffer)
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		wc.FlushAndWait(flushCtx)
		flushCancel()

		completeCtx, completeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		errMsg := ""
		if runErr != nil {
			errMsg = runErr.Error()
		}
		if cerr := wc.ChunkComplete(completeCtx, chunk.ChunkID, submitted, errMsg); cerr != nil {
			log.Printf("chunk-complete: %v", cerr)
		}
		completeCancel()
		log.Printf("chunk %d done: submitted=%d err=%q", chunk.ChunkID, submitted, errMsg)
	}

	close(heartbeatStop)
	wc.Close()
}

// runChunk boots a fresh scanner pipeline for the chunk and returns
// the number of results submitted. Always submits all results before
// returning. A non-nil error indicates a soft failure (server will
// requeue subject to attempt cap).
func runChunk(parent context.Context, wc *wcClient, chunk claimResponse, jobBuffer int) (int, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	cfg := scannerConfig{
		Workers:     chunk.ScanWorkers,
		DialTimeout: time.Duration(chunk.DialTimeoutMs) * time.Millisecond,
		ReadTimeout: time.Duration(chunk.ReadTimeoutMs) * time.Millisecond,
		BodyLimit:   int64(chunk.BodyLimit),
		UserAgent:   chunk.UserAgent,
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 500
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 2 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.BodyLimit <= 0 {
		cfg.BodyLimit = 1 << 20
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "scrap-metal-worker/1"
	}

	jobsCh := make(chan scanJob, jobBuffer)
	resultsCh := make(chan scanResult, jobBuffer)

	go func() {
		defer close(jobsCh)
		for _, p := range chunk.Pairs {
			ip, err := netip.ParseAddr(p.IP)
			if err != nil {
				log.Printf("runChunk: skipping bad ip %q: %v", p.IP, err)
				continue
			}
			select {
			case <-ctx.Done():
				return
			case jobsCh <- scanJob{IP: ip, Port: p.Port}:
			}
		}
	}()

	var workersWG sync.WaitGroup
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		scannerRun(ctx, cfg, jobsCh, resultsCh)
		close(resultsCh)
	}()

	var (
		submitted atomic.Int64
		open      atomic.Int64
		httpHits  atomic.Int64
	)
	for r := range resultsCh {
		if r.PortOpen {
			open.Add(1)
		}
		if r.HTTPStatus != nil {
			httpHits.Add(1)
		}
		wc.SubmitWithChunk(r, chunk.ChunkID)
		submitted.Add(1)
	}
	workersWG.Wait()

	log.Printf("chunk %d: scanned=%d open=%d http=%d",
		chunk.ChunkID, submitted.Load(), open.Load(), httpHits.Load())
	return int(submitted.Load()), nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
