package engine

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// outcome содержит ровно один терминальный исход запроса без тела и чувствительных данных.
type outcome struct {
	status  int
	kind    string
	elapsed time.Duration
}

// newClient создаёт отдельный пул соединений без redirects и прозрачного повтора запросов.
func newClient(config Config) *http.Client {
	transport := &http.Transport{
		Proxy:                  http.ProxyFromEnvironment,
		DialContext:            (&net.Dialer{Timeout: time.Duration(config.RequestTimeoutMs) * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:           config.MaxConcurrency,
		MaxIdleConnsPerHost:    config.MaxConcurrency,
		MaxConnsPerHost:        config.MaxConcurrency,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    time.Duration(config.RequestTimeoutMs) * time.Millisecond,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 64 << 10,
		DisableCompression:     true,
		// HTTP/1.1 avoids HTTP/2 stream-level automatic retries.
		Protocols: new(http.Protocols),
	}
	transport.Protocols.SetHTTP1(true)
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// perform читает не более лимита плюс один байт и всегда закрывает тело ответа.
func perform(ctx context.Context, client *http.Client, config Config) outcome {
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(config.RequestTimeoutMs)*time.Millisecond)
	defer cancel()
	// Ненулевой неповторяемый Body запрещает Transport повторять запрос на reused connection,
	// даже для GET без полезной нагрузки. GetBody намеренно отсутствует.
	request, err := http.NewRequestWithContext(requestCtx, config.Method, config.URL, io.NopCloser(strings.NewReader(config.Body)))
	if err != nil {
		return outcome{kind: "transport"}
	}
	request.ContentLength = int64(len(config.Body))
	for name, value := range config.Headers {
		if http.CanonicalHeaderKey(name) == "Host" {
			request.Host = value
		} else {
			request.Header.Set(name, value)
		}
	}
	started := time.Now()
	response, err := client.Do(request)
	var count int64
	if response != nil {
		if err == nil {
			count, err = io.Copy(io.Discard, io.LimitReader(response.Body, config.MaxResponseBodyBytes+1))
		}
		response.Body.Close()
	}
	result := outcome{elapsed: time.Since(started)}
	switch {
	case err != nil:
		switch {
		case errors.Is(err, context.Canceled):
			result.kind = "cancelled"
		case errors.Is(err, context.DeadlineExceeded):
			result.kind = "timeout"
		default:
			result.kind = "transport"
		}
	case count > config.MaxResponseBodyBytes:
		result.kind = "bodyLimit"
	default:
		result.status = response.StatusCode
	}
	return result
}
