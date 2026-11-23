package engine

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// Engine владеет одним выполнением нагрузки и ограниченной очередью секундных интервалов.
// Получатель читает Intervals до закрытия; медленный получатель вызывает MeasurementsLost, но не тормозит нагрузку.
type Engine struct {
	intervals chan Interval
	used      atomic.Bool
}

// New создаёт движок; очередь вмещает до 16 секунд измерений независимо от длительности запуска.
func New() *Engine {
	return &Engine{intervals: make(chan Interval, 16)}
}

// Intervals возвращает поток закрытых секундных интервалов; последний интервал находится в Result.Final.
func (e *Engine) Intervals() <-chan Interval { return e.intervals }

// Run выполняется один раз на экземпляр Engine. Отмена прекращает генерацию; запросы получают drainTimeoutMs на завершение.
// Метод дожидается всех собственных горутин, включая отменённые по истечении drain запросы.
func (e *Engine) Run(ctx context.Context, config Config) (Result, error) {
	return e.run(ctx, config, perform)
}

// run позволяет проверять планировщик без реального сетевого ввода-вывода.
func (e *Engine) run(ctx context.Context, config Config, execute func(context.Context, *http.Client, Config) outcome) (Result, error) {
	return e.runWithClock(ctx, config, execute, time.Now)
}

// runWithClock позволяет задержать пробуждение планировщика в тесте с управляемыми часами.
func (e *Engine) runWithClock(ctx context.Context, config Config, execute func(context.Context, *http.Client, Config) outcome, nowTime func() time.Time) (Result, error) {
	return e.runScheduled(ctx, config, execute, nowTime, time.Time{}, nil)
}

// RunScheduled сохраняет сетку слотов координатора даже при позднем пробуждении агента.
func (e *Engine) RunScheduled(ctx context.Context, config Config, start time.Time, onStart func(time.Time)) (Result, error) {
	return e.runScheduled(ctx, config, perform, time.Now, start, onStart)
}

// runScheduled запускает общий планировщик с переданной или локальной точкой отсчёта.
func (e *Engine) runScheduled(ctx context.Context, config Config, execute func(context.Context, *http.Client, Config) outcome,
	nowTime func() time.Time, start time.Time, onStart func(time.Time)) (Result, error) {
	if !e.used.CompareAndSwap(false, true) {
		return Result{}, fmt.Errorf("engine can only run once")
	}
	defer close(e.intervals)
	if err := config.Validate(); err != nil {
		return Result{}, err
	}
	// Копия заголовков исключает изменение snapshot вызывающим кодом во время выполнения.
	headers := make(map[string]string, len(config.Headers))
	for key, value := range config.Headers {
		headers[key] = value
	}
	config.Headers = headers
	client := newClient(config)
	defer client.CloseIdleConnections()
	requestsCtx, cancelRequests := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRequests()
	outcomes := make(chan outcome, config.MaxConcurrency)
	if start.IsZero() || ctx.Err() != nil {
		start = nowTime()
	}
	end := start.Add(time.Duration(config.DurationMs) * time.Millisecond)
	result := Result{PlannedRequests: config.PlannedRequests()}
	interval := newInterval(start.UTC())
	if ctx.Err() != nil {
		result.Stopped = true
		result.GenerationStoppedAt = start.UTC()
		result.FinishedAt = start.UTC()
		interval.EndAt = start.UTC()
		result.Final = interval
		return result, nil
	}
	boundary := start.Add(time.Second)
	active := 0
	nextSlot := uint64(0)
	generating := true
	var drainDeadline time.Time
	stopSignal := ctx.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()

	// skipUntil относит просроченные слоты к текущему интервалу.
	skipUntil := func(at time.Time) {
		count := min(result.PlannedRequests, slotsBefore(at.Sub(start), config.RPS))
		if count <= nextSlot {
			return
		}
		missed := count - nextSlot
		interval.Counters.Scheduled += missed
		interval.Counters.Skipped += missed
		result.Totals.Scheduled += missed
		result.Totals.Skipped += missed
		nextSlot = count
	}
	// flush закрывает интервалы по монотонным часам; поздний исход попадает в текущий интервал.
	flush := func(now time.Time) {
		for !now.Before(boundary) {
			if generating {
				skipUntil(minTime(boundary, end))
			}
			interval.EndAt = start.UTC().Add(boundary.Sub(start))
			select {
			case e.intervals <- interval:
			default:
				result.MeasurementsLost = true
			}
			interval = newInterval(interval.EndAt)
			boundary = boundary.Add(time.Second)
		}
	}
	// stop сохраняет фактическое окно генерации без времени drain.
	stop := func(at time.Time, manual bool) {
		at = minTime(at, end)
		flush(at)
		skipUntil(at)
		generating = false
		stopSignal = nil
		result.Stopped = manual
		result.GenerationStoppedAt = start.UTC().Add(at.Sub(start))
		result.GenerationDurationUs = uint64((at.Sub(start) + time.Microsecond - 1) / time.Microsecond)
		drainDeadline = nowTime().Add(time.Duration(config.DrainTimeoutMs) * time.Millisecond)
	}

	for {
		now := nowTime()
		if generating {
			if ctx.Err() != nil {
				stop(now, true)
			} else if !now.Before(end) {
				// Позднее пробуждение учитывает оставшийся план как skipped, без отправки запросов после конца.
				stop(end, false)
			}
		}
		flush(now)
		if generating {
			if !now.Before(start.Add(slotOffset(nextSlot, config.RPS))) {
				if result.ActualStartAt == nil {
					actual := start.UTC().Add(now.Sub(start))
					result.ActualStartAt = &actual
					if onStart != nil {
						onStart(actual)
					}
				}
				due := slotsBefore(now.Sub(start)+time.Nanosecond, config.RPS) - 1
				if due >= result.PlannedRequests {
					due = result.PlannedRequests - 1
				}
				missed := due - nextSlot
				interval.Counters.Scheduled += missed + 1
				interval.Counters.Skipped += missed
				result.Totals.Scheduled += missed + 1
				result.Totals.Skipped += missed
				nextSlot = due + 1
				if active < config.MaxConcurrency {
					active++
					interval.Counters.Started++
					result.Totals.Started++
					go func() { outcomes <- execute(requestsCtx, client, config) }()
				} else {
					interval.Counters.Skipped++
					result.Totals.Skipped++
				}
			}
		}
		if !generating && active == 0 {
			break
		}
		deadline := boundary
		if generating {
			slot := start.Add(slotOffset(nextSlot, config.RPS))
			if nextSlot >= result.PlannedRequests {
				slot = end
			}
			if slot.Before(deadline) {
				deadline = slot
			}
		} else if !drainDeadline.IsZero() && drainDeadline.Before(deadline) {
			deadline = drainDeadline
		}
		timer.Reset(max(time.Duration(0), deadline.Sub(nowTime())))
		select {
		case <-stopSignal:
			stop(nowTime(), true)
		case item := <-outcomes:
			flush(nowTime())
			active--
			interval.add(item)
			addOutcome(&result.Totals, item)
		case <-timer.C:
			if !generating && !drainDeadline.IsZero() && !nowTime().Before(drainDeadline) {
				cancelRequests()
				drainDeadline = time.Time{}
			}
		}
	}
	finished := nowTime()
	flush(finished)
	result.FinishedAt = start.UTC().Add(finished.Sub(start))
	interval.EndAt = result.FinishedAt
	result.Final = interval
	return result, nil
}

// minTime выбирает ближайшую границу без потери монотонной составляющей.
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// add относит завершение к текущему интервалу и соответствующей гистограмме.
func (i *Interval) add(item outcome) {
	addOutcome(&i.Counters, item)
	if item.kind == "" {
		i.HTTPStatusCounts[item.status]++
		i.HTTPLatency.record(item.elapsed)
	} else {
		i.FailureLatency.record(item.elapsed)
	}
}

// addOutcome учитывает терминальный исход ровно один раз, отделяя HTTP-ошибки от сетевых.
func addOutcome(c *Counters, item outcome) {
	c.Finished++
	switch item.kind {
	case "cancelled":
		c.Cancelled++
	case "timeout":
		c.Timeouts++
	case "transport":
		c.TransportErrors++
	case "bodyLimit":
		c.ResponseBodyLimitExceeded++
	default:
		c.HTTPResponses++
		if item.status >= 400 {
			c.HTTPErrors++
		}
	}
}
