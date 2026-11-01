package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouterSendsPrefixToItsBackend(t *testing.T) {
	serve := func(name string) *httptest.Server {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(name))
		}))
		t.Cleanup(s.Close)
		return s
	}
	users, orders := serve("users"), serve("orders")

	router, err := newRouter([]Route{
		{Prefix: "/users/", Backend: users.URL},
		{Prefix: "/orders/", Backend: orders.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{"/users/1": "users", "/orders/1": "orders"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Body.String() != want {
			t.Fatalf("%s reached %q, want %q", path, w.Body.String(), want)
		}
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nothing", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("unrouted path returned %d", w.Code)
	}
}
