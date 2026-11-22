//go:build integration

package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestHTTPOutcomes проверяет реальные HTTP-ответы и отдельные категории неуспешных исходов.
func TestHTTPOutcomes(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		limit   int64
		status  int
		kind    string
	}{
		{"exact body limit", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "1234") }, 4, 200, ""},
		{"body too large", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "12345") }, 4, 0, "bodyLimit"},
		{"HTTP error", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }, 1024, 503, ""},
		{"redirect disabled", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", "/followed")
			w.WriteHeader(302)
		}, 1024, 302, ""},
		{"timeout", func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }, 1024, 0, "timeout"},
		{"truncated body", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "100")
			io.WriteString(w, "short")
		}, 1024, 0, "transport"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				io.Copy(io.Discard, r.Body)
				tc.handler(w, r)
			}))
			defer server.Close()
			config := testConfig()
			config.URL = server.URL
			config.MaxResponseBodyBytes = tc.limit
			config.RequestTimeoutMs = 100
			client := newClient(config)
			defer client.CloseIdleConnections()
			got := perform(t.Context(), client, config)
			if got.status != tc.status || got.kind != tc.kind || calls.Load() != 1 {
				t.Fatalf("outcome=%+v calls=%d", got, calls.Load())
			}
		})
	}
}

// TestHTTPReuse проверяет повторное использование соединения и передачу заданных метода, тела и заголовков.
func TestHTTPReuse(t *testing.T) {
	addresses := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || string(body) != "payload" || r.Header.Get("X-Test") != "value" {
			w.WriteHeader(400)
			return
		}
		addresses <- r.RemoteAddr
		io.WriteString(w, "ok")
	}))
	defer server.Close()
	config := testConfig()
	config.URL = server.URL
	config.Method = "POST"
	config.Body = "payload"
	config.Headers = map[string]string{"X-Test": "value"}
	client := newClient(config)
	defer client.CloseIdleConnections()
	for range 2 {
		if got := perform(t.Context(), client, config); got.status != 200 {
			t.Fatalf("outcome: %+v", got)
		}
	}
	if first, second := <-addresses, <-addresses; first != second {
		t.Fatalf("connection not reused: %s %s", first, second)
	}
}

// TestNoRetryOnReusedConnection обрывает второй запрос после получения сервером и запрещает третью отправку.
func TestNoRetryOnReusedConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer connection.Close()
		connection.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(connection)
		for index := 0; index < 2; index++ {
			request, err := http.ReadRequest(reader)
			if err != nil {
				done <- err
				return
			}
			io.Copy(io.Discard, request.Body)
			request.Body.Close()
			if index == 0 {
				fmt.Fprint(connection, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
			}
		}
		connection.Close()
		listener.(*net.TCPListener).SetDeadline(time.Now().Add(250 * time.Millisecond))
		retry, err := listener.Accept()
		if err == nil {
			retry.Close()
			done <- fmt.Errorf("unexpected retry connection")
			return
		}
		if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			done <- err
			return
		}
		done <- nil
	}()
	config := testConfig()
	config.URL = "http://" + listener.Addr().String()
	config.RequestTimeoutMs = 1000
	client := newClient(config)
	defer client.CloseIdleConnections()
	if first := perform(t.Context(), client, config); first.status != 200 {
		t.Fatalf("first outcome: %+v", first)
	}
	if second := perform(t.Context(), client, config); second.kind != "transport" {
		t.Fatalf("second outcome: %+v", second)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestHTTPDrain проверяет отмену медленного реального HTTP-запроса по deadline drain.
func TestHTTPDrain(t *testing.T) {
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	config := testConfig()
	config.URL = server.URL
	config.RPS = 1
	config.DurationMs = 10000
	config.DrainTimeoutMs = 50
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		select {
		case <-started:
			cancel()
		case <-ctx.Done():
		}
	}()
	generator := New()
	result, err := generator.Run(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Stopped || result.Totals.Started != 1 || result.Totals.Cancelled != 1 {
		t.Fatalf("result: %+v", result)
	}
	assertIntervals(t, generator, result)
}
