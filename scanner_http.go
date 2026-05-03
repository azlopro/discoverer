package main

import (
	"net"
	"net/http"
	"time"
)

// newHTTPClient returns a per-worker HTTP client with no keep-alive (every
// target is unique) and no automatic redirect following.
func newHTTPClient(dialTimeout, readTimeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: -1,
	}
	tr := &http.Transport{
		DialContext:           dialer.DialContext,
		DisableKeepAlives:     true,
		DisableCompression:    true,
		ResponseHeaderTimeout: readTimeout,
		TLSHandshakeTimeout:   readTimeout,
		ExpectContinueTimeout: 500 * time.Millisecond,
		MaxIdleConns:          0,
		MaxIdleConnsPerHost:   -1,
	}
	return &http.Client{
		Transport: tr,
		Timeout:   dialTimeout + readTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
