package engine

import (
	"testing"
	"time"
)

// TestValidate проверяет отклонение неверных и чрезмерных параметров до генерации нагрузки.
func TestValidate(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"URL", func(c *Config) { c.URL = "file:///tmp/test" }},
		{"credentials", func(c *Config) { c.URL = "http://user:secret@localhost/" }},
		{"method", func(c *Config) { c.Method = "CONNECT" }},
		{"RPS", func(c *Config) { c.RPS = 0 }},
		{"duration", func(c *Config) { c.DurationMs = 86400001 }},
		{"timeout", func(c *Config) { c.RequestTimeoutMs = 0 }},
		{"drain", func(c *Config) { c.DrainTimeoutMs = 0 }},
		{"concurrency", func(c *Config) { c.MaxConcurrency = 100001 }},
		{"body limit", func(c *Config) { c.MaxResponseBodyBytes = 0 }},
		{"header injection", func(c *Config) { c.Headers = map[string]string{"X-Test": "value\r\nInjected: yes"} }},
		{"header name", func(c *Config) { c.Headers = map[string]string{"Bad Name": "value"} }},
		{"transport header", func(c *Config) { c.Headers = map[string]string{"Content-Length": "0"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := testConfig()
			tc.change(&config)
			if config.Validate() == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

// TestScheduleArithmetic проверяет дробный RPS-период и максимальный суточный план без переполнения.
func TestScheduleArithmetic(t *testing.T) {
	config := testConfig()
	config.RPS = 3
	config.DurationMs = 1001
	if config.PlannedRequests() != 4 || slotOffset(3, 3) != time.Second || slotsBefore(time.Second, 3) != 3 {
		t.Fatal("fractional plan changed")
	}
	config.RPS = 1_000_000
	config.DurationMs = 86_400_000
	if config.PlannedRequests() != 86_400_000_000 || slotOffset(config.PlannedRequests(), config.RPS) != 24*time.Hour || slotsBefore(24*time.Hour, config.RPS) != config.PlannedRequests() {
		t.Fatal("maximum plan overflow")
	}
}
