package main

import (
	"net/netip"
	"time"
)

// scanResult is one (ip, port) probe outcome produced by the scanner and
// shipped to the server by the workerclient. Same shape as the server's
// internal/store.Result minus the WorkerID column (the server attaches
// that from the auth context on ingest).
type scanResult struct {
	IP          netip.Addr
	Port        int
	ScannedAt   time.Time
	PortOpen    bool
	HTTPStatus  *int
	Headers     map[string][]string
	Body        []byte
	BodySize    *int
	ContentType string
	Server      string
	Title       string
	Error       string
	DurationMs  int
}
