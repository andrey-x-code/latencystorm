package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/andrey-x-code/latencystorm/src/load-agent/internal/engine"
)

// readLocalConfig читает единственный ограниченный JSON-документ без неизвестных полей.
func readLocalConfig(reader io.Reader) (engine.Config, error) {
	decoder := json.NewDecoder(io.LimitReader(reader, 2<<20))
	decoder.DisallowUnknownFields()
	var config engine.Config
	if err := decoder.Decode(&config); err != nil {
		return config, fmt.Errorf("read local configuration: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return config, fmt.Errorf("configuration must contain exactly one JSON object")
	}
	return config, config.Validate()
}

// runLocal выводит локальные NDJSON-измерения; ошибка вывода отменяет генерацию и дожидается drain.
// Локальный формат предназначен для диагностики, а не для публикации Kafka ResultBatch.
func runLocal(ctx context.Context, config engine.Config, output io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	generator := engine.New()
	type completion struct {
		result engine.Result
		err    error
	}
	done := make(chan completion, 1)
	go func() {
		result, err := generator.Run(ctx, config)
		done <- completion{result, err}
	}()
	encoder := json.NewEncoder(output)
	var writeError error
	for interval := range generator.Intervals() {
		if writeError == nil {
			writeError = encoder.Encode(struct {
				Type     string          `json:"type"`
				Interval engine.Interval `json:"interval"`
			}{"interval", interval})
			if writeError != nil {
				cancel()
			}
		}
	}
	finished := <-done
	if writeError != nil {
		return fmt.Errorf("write measurements: %w", writeError)
	}
	if finished.err != nil {
		return finished.err
	}
	return encoder.Encode(struct {
		Type   string        `json:"type"`
		Result engine.Result `json:"result"`
	}{"summary", finished.result})
}

// localMain запускает автономную нагрузку и обрабатывает SIGINT/SIGTERM без зависимостей инфраструктуры.
func localMain(args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: load-agent --local <config.json>")
		return 2
	}
	file, err := os.Open(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot open local configuration")
		return 1
	}
	config, err := readLocalConfig(file)
	file.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runLocal(ctx, config, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
