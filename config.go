package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	WorkerID       string
	APIKey         string
	ServerURL      string
	SpoolDir       string
	HeartbeatEvery time.Duration
	IdlePoll       time.Duration

	// Compress gzips /results POST bodies. Default on; safe to disable
	// when targeting a server that pre-dates Content-Encoding support.
	Compress bool

	// SpoolReplayInterval re-tries the spool dir on a cadence. 0
	// disables (replay then runs once at startup only).
	SpoolReplayInterval time.Duration

	// Local result-batching knobs. Hard-coded here (not env-tunable);
	// chunks are bounded server-side so these don't need per-deploy
	// tuning anymore.
	BatchSize     int
	BatchInterval time.Duration

	// JobBuffer sizes the scanner channels per chunk. Set high enough
	// that scannerRun never blocks on a full result chan.
	JobBuffer int
}

// loadConfig parses CLI flags falling back to env vars and then to
// hardcoded defaults. Precedence: flag > env > default.
func loadConfig(args []string) (*config, error) {
	fs := flag.NewFlagSet("scrap-metal-worker", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(),
			"scrap-metal-worker — polls a server for chunks and ships scan results back.\n\n"+
				"Flags (each falls back to its env var, then to its default):\n\n")
		fs.PrintDefaults()
	}

	id := fs.String("id", envStr("WORKER_ID", ""),
		"Worker ID (env WORKER_ID; defaults to hostname)")
	apiKey := fs.String("api-key", envStr("WORKER_API_KEY", ""),
		"Bearer token from `server issue-key` (env WORKER_API_KEY; required)")
	serverURL := fs.String("server", envStr("SERVER_URL", ""),
		"Server base URL, e.g. https://hello.azlo.pro (env SERVER_URL; required)")
	spoolDir := fs.String("spool-dir", envStr("WORKER_SPOOL_DIR", "./spool"),
		"Local dir for offline result spool (env WORKER_SPOOL_DIR)")
	heartbeat := fs.Duration("heartbeat", envDur("WORKER_HEARTBEAT", 60*time.Second),
		"Heartbeat interval; 0 disables (env WORKER_HEARTBEAT)")
	idlePoll := fs.Duration("idle-poll", envDur("WORKER_IDLE_POLL", 10*time.Second),
		"Sleep between empty claim polls (env WORKER_IDLE_POLL)")
	spoolReplay := fs.Duration("spool-replay", envDur("WORKER_SPOOL_REPLAY", 5*time.Minute),
		"Periodic spool replay interval; 0 disables, replay then runs only at startup (env WORKER_SPOOL_REPLAY)")
	compress := fs.Bool("compress", envBool("WORKER_COMPRESS", true),
		"gzip /results POST bodies (env WORKER_COMPRESS)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	c := &config{
		WorkerID:            *id,
		APIKey:              *apiKey,
		ServerURL:           *serverURL,
		SpoolDir:            *spoolDir,
		HeartbeatEvery:      *heartbeat,
		IdlePoll:            *idlePoll,
		Compress:            *compress,
		SpoolReplayInterval: *spoolReplay,
		BatchSize:           200,
		BatchInterval:       5 * time.Second,
		JobBuffer:           10000,
	}
	if c.WorkerID == "" {
		host, _ := os.Hostname()
		c.WorkerID = strings.TrimSpace(host)
	}
	if c.WorkerID == "" {
		return nil, fmt.Errorf("worker id is required (--id, WORKER_ID, or a non-empty hostname)")
	}
	if c.APIKey == "" {
		return nil, fmt.Errorf("api key is required (--api-key or WORKER_API_KEY)")
	}
	if c.ServerURL == "" {
		return nil, fmt.Errorf("server URL is required (--server or SERVER_URL)")
	}
	return c, nil
}

func envStr(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

//nolint:unused // retained for future env-tunable knobs
func envInt(k string, def int) int {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
