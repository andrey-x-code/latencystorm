// Package engine выполняет HTTP-нагрузку независимо от транспорта управления и результатов.
package engine

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Config задаёт нагрузку одного агента; длительности указаны в целых миллисекундах.
type Config struct {
	Method               string            `json:"method"`
	URL                  string            `json:"url"`
	Headers              map[string]string `json:"headers"`
	Body                 string            `json:"body"`
	RPS                  int               `json:"rps"`
	DurationMs           int               `json:"durationMs"`
	RequestTimeoutMs     int               `json:"requestTimeoutMs"`
	MaxConcurrency       int               `json:"maxConcurrency"`
	MaxResponseBodyBytes int64             `json:"maxResponseBodyBytes"`
	DrainTimeoutMs       int               `json:"drainTimeoutMs"`
}

// Validate проверяет границы ресурсов и HTTP-параметры до запуска горутин.
func (c Config) Validate() error {
	address, err := url.Parse(c.URL)
	if err != nil || address.Hostname() == "" || (address.Scheme != "http" && address.Scheme != "https") || address.User != nil || address.Fragment != "" {
		return fmt.Errorf("url must be an absolute HTTP/HTTPS URL without credentials or fragment")
	}
	switch c.Method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD":
	default:
		return fmt.Errorf("unsupported HTTP method")
	}
	if c.RPS < 1 || c.RPS > 1_000_000 || c.DurationMs < 1 || c.DurationMs > 86_400_000 {
		return fmt.Errorf("rps must be 1..1000000 and durationMs 1..86400000")
	}
	if c.RequestTimeoutMs < 1 || c.RequestTimeoutMs > 300_000 || c.DrainTimeoutMs < 1 || c.DrainTimeoutMs > 300_000 {
		return fmt.Errorf("requestTimeoutMs and drainTimeoutMs must be 1..300000")
	}
	if c.MaxConcurrency < 1 || c.MaxConcurrency > 100_000 || c.MaxResponseBodyBytes < 1 || c.MaxResponseBodyBytes > 16<<20 {
		return fmt.Errorf("maxConcurrency must be 1..100000 and maxResponseBodyBytes 1..16777216")
	}
	if len(c.Body) > 1<<20 || len(c.Headers) > 100 {
		return fmt.Errorf("body must be at most 1 MiB and headers at most 100 entries")
	}
	for name, value := range c.Headers {
		if len(name) == 0 || len(name) > 256 || len(value) > 8192 || strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("invalid header name or value")
		}
		for _, char := range name {
			if !strings.ContainsRune("!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", char) {
				return fmt.Errorf("invalid header name")
			}
		}
		switch http.CanonicalHeaderKey(name) {
		case "Content-Length", "Transfer-Encoding", "Connection", "Trailer", "Upgrade":
			return fmt.Errorf("transport header %s is not configurable", name)
		}
	}
	_, err = http.NewRequest(c.Method, c.URL, nil)
	if err != nil {
		return fmt.Errorf("invalid request: %w", err)
	}
	return nil
}

// PlannedRequests возвращает полный план с округлением вверх без float.
func (c Config) PlannedRequests() uint64 {
	return (uint64(c.RPS)*uint64(c.DurationMs) + 999) / 1000
}

// slotOffset сохраняет исходную сетку, не накапливая ошибку округления периода.
func slotOffset(slot uint64, rps int) time.Duration {
	rate := uint64(rps)
	return time.Duration(slot/rate)*time.Second + time.Duration(((slot%rate)*uint64(time.Second)+rate-1)/rate)
}

// slotsBefore считает слоты строго до указанного смещения, не переполняя произведение за сутки.
func slotsBefore(elapsed time.Duration, rps int) uint64 {
	if elapsed <= 0 {
		return 0
	}
	return uint64(elapsed/time.Second)*uint64(rps) + (uint64(elapsed%time.Second)*uint64(rps)+uint64(time.Second)-1)/uint64(time.Second)
}
