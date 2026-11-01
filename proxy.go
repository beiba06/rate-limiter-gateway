package main

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

// The default transport keeps only 2 idle connections per host, which churns TCP
// connections under load and risks exhausting ephemeral ports.
var backendTransport = &http.Transport{
	MaxIdleConns:        1024,
	MaxIdleConnsPerHost: 512,
	IdleConnTimeout:     90 * time.Second,
}

func newRouter(routes []Route) (http.Handler, error) {
	mux := http.NewServeMux()
	for _, route := range routes {
		target, err := url.Parse(route.Backend)
		if err != nil {
			return nil, fmt.Errorf("route %s: %w", route.Prefix, err)
		}
		mux.Handle(route.Prefix, &httputil.ReverseProxy{
			Transport: backendTransport,
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(target)
				r.SetXForwarded()
			},
		})
	}
	return mux, nil
}
