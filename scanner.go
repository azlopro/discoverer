package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type scanJob struct {
	IP   netip.Addr
	Port int
}

type scannerConfig struct {
	Workers     int
	DialTimeout time.Duration
	ReadTimeout time.Duration
	BodyLimit   int64
	UserAgent   string
}

// scannerRun drains jobs and pushes results into out. Blocks until jobs is
// closed AND every worker has finished. Caller closes jobs when input is
// exhausted; caller is responsible for routing results onward.
func scannerRun(ctx context.Context, cfg scannerConfig, jobs <-chan scanJob, out chan<- scanResult) {
	var wg sync.WaitGroup
	wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go func() {
			defer wg.Done()
			client := newHTTPClient(cfg.DialTimeout, cfg.ReadTimeout)
			for {
				select {
				case <-ctx.Done():
					return
				case j, ok := <-jobs:
					if !ok {
						return
					}
					out <- probe(ctx, client, j, cfg)
				}
			}
		}()
	}
	wg.Wait()
}

func probe(ctx context.Context, client *http.Client, j scanJob, cfg scannerConfig) scanResult {
	start := time.Now()
	r := scanResult{
		IP:        j.IP,
		Port:      j.Port,
		ScannedAt: start.UTC(),
	}

	addr := net.JoinHostPort(j.IP.String(), strconv.Itoa(j.Port))

	reqCtx, cancelReq := context.WithTimeout(ctx, cfg.DialTimeout+cfg.ReadTimeout)
	defer cancelReq()

	url := "http://" + addr + "/"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		r.Error = trimErr(err)
		r.DurationMs = int(time.Since(start) / time.Millisecond)
		return r
	}
	req.Header.Set("User-Agent", cfg.UserAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "identity")
	req.Host = j.IP.String()

	resp, err := client.Do(req)
	if err != nil {
		// A dial error means TCP never connected → port closed or
		// filtered. Anything else (read timeout, malformed response,
		// peer reset mid-response) means the port WAS open and we
		// just failed at a later phase.
		var opErr *net.OpError
		r.PortOpen = !(errors.As(err, &opErr) && opErr.Op == "dial")
		r.Error = trimErr(err)
		r.DurationMs = int(time.Since(start) / time.Millisecond)
		return r
	}
	defer resp.Body.Close()
	r.PortOpen = true

	status := resp.StatusCode
	r.HTTPStatus = &status
	r.Headers = map[string][]string(resp.Header)
	r.ContentType = resp.Header.Get("Content-Type")
	r.Server = resp.Header.Get("Server")

	// io.ReadAll returns nil error on EOF, so any non-nil err is real.
	// Connections aren't reused (DisableKeepAlives), so we don't bother
	// draining beyond BodyLimit.
	body, err := io.ReadAll(io.LimitReader(resp.Body, cfg.BodyLimit))
	if err != nil {
		r.Error = "body: " + trimErr(err)
	}

	if len(body) > 0 {
		r.Body = body
		size := len(body)
		r.BodySize = &size
		if isHTMLish(r.ContentType) {
			r.Title = extractTitle(body)
		}
	}

	r.DurationMs = int(time.Since(start) / time.Millisecond)
	return r
}

var titleRe = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)

func extractTitle(body []byte) string {
	m := titleRe.FindSubmatch(body)
	if len(m) < 2 {
		return ""
	}
	t := strings.TrimSpace(string(m[1]))
	if len(t) > 512 {
		t = t[:512]
	}
	return t
}

func isHTMLish(ct string) bool {
	ct = strings.ToLower(ct)
	return strings.Contains(ct, "html") || strings.Contains(ct, "xml") || ct == ""
}

func trimErr(err error) string {
	s := err.Error()
	if len(s) > 256 {
		return s[:256]
	}
	return s
}

