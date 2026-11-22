package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/andrey-x-code/latencystorm/src/load-agent/internal/engine"
)

const localConfigJSON = `{"method":"GET","url":"http://localhost/","headers":{},"rps":10,"durationMs":1000,"requestTimeoutMs":100,"maxConcurrency":1,"maxResponseBodyBytes":1024,"drainTimeoutMs":100}`

// TestReadLocalConfig проверяет строгий разбор конфигурации без запуска нагрузки.
func TestReadLocalConfig(t *testing.T) {
	cases := []struct {
		name, input string
		valid       bool
	}{
		{"valid", localConfigJSON, true},
		{"unknown field", strings.Replace(localConfigJSON, `"rps"`, `"unknown"`, 1), false},
		{"trailing object", localConfigJSON + " {}", false},
		{"invalid rate", strings.Replace(localConfigJSON, `"rps":10`, `"rps":0`, 1), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readLocalConfig(strings.NewReader(tc.input))
			if (err == nil) != tc.valid {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

// TestRunLocalCancelled проверяет итоговый NDJSON при отмене до первого запроса.
func TestRunLocalCancelled(t *testing.T) {
	config, err := readLocalConfig(strings.NewReader(localConfigJSON))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var output bytes.Buffer
	if err := runLocal(ctx, config, &output); err != nil {
		t.Fatal(err)
	}
	var record struct {
		Type   string
		Result engine.Result
	}
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.Type != "summary" || !record.Result.Stopped || record.Result.Totals.Started != 0 {
		t.Fatalf("record: %+v", record)
	}
}
