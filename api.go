// Wire types shared with the central server. Keep in sync with
// internal/api/types.go in the parent project — drift here means the
// worker silently fails to ingest. Worker has its own go.mod so it
// cannot import the internal/api package directly.
package main

import (
	"encoding/json"
	"fmt"
	"time"
)

type ingestRequest struct {
	WorkerID string         `json:"worker_id"`
	ChunkID  int64          `json:"chunk_id,omitempty"`
	Batch    []resultRecord `json:"batch"`
}

type resultRecord struct {
	IP          string              `json:"ip"`
	Port        int                 `json:"port"`
	ScannedAt   time.Time           `json:"scanned_at"`
	PortOpen    bool                `json:"port_open"`
	HTTPStatus  *int                `json:"http_status,omitempty"`
	Headers     map[string][]string `json:"headers,omitempty"`
	Body        []byte              `json:"body,omitempty"`
	BodySize    *int                `json:"body_size,omitempty"`
	ContentType string              `json:"content_type,omitempty"`
	Server      string              `json:"server,omitempty"`
	Title       string              `json:"title,omitempty"`
	Error       string              `json:"error,omitempty"`
	DurationMs  int                 `json:"duration_ms"`
}

// Pair is a single (ip, port) probe target. Encoded on the wire as a
// 2-element JSON array (e.g. ["1.2.3.4", 80]).
type Pair struct {
	IP   string
	Port int
}

func (p Pair) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]any{p.IP, p.Port})
}

func (p *Pair) UnmarshalJSON(data []byte) error {
	var raw [2]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("pair: %w", err)
	}
	if err := json.Unmarshal(raw[0], &p.IP); err != nil {
		return fmt.Errorf("pair ip: %w", err)
	}
	if err := json.Unmarshal(raw[1], &p.Port); err != nil {
		return fmt.Errorf("pair port: %w", err)
	}
	return nil
}

type claimRequest struct {
	WorkerID string `json:"worker_id"`
}

type claimResponse struct {
	Empty          bool      `json:"empty,omitempty"`
	ChunkID        int64     `json:"chunk_id,omitempty"`
	JobID          int64     `json:"job_id,omitempty"`
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty"`
	Pairs          []Pair    `json:"pairs,omitempty"`

	DialTimeoutMs int    `json:"dial_timeout_ms,omitempty"`
	ReadTimeoutMs int    `json:"read_timeout_ms,omitempty"`
	BodyLimit     int    `json:"body_limit,omitempty"`
	ScanWorkers   int    `json:"scan_workers,omitempty"`
	UserAgent     string `json:"user_agent,omitempty"`
}

type chunkCompleteRequest struct {
	WorkerID  string `json:"worker_id"`
	ChunkID   int64  `json:"chunk_id"`
	Submitted int    `json:"submitted"`
	Error     string `json:"error,omitempty"`
}

const (
	pathResults       = "/api/v1/results"
	pathHeartbeat     = "/api/v1/heartbeat"
	pathClaim         = "/api/v1/claim"
	pathChunkComplete = "/api/v1/chunk-complete"
)
