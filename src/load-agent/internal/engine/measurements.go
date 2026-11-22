package engine

import "time"

// Counters содержит дельты одного интервала; в локальном JSON uint64 представлены строками.
type Counters struct {
	Scheduled                 uint64 `json:"scheduled,string"`
	Started                   uint64 `json:"started,string"`
	Skipped                   uint64 `json:"skipped,string"`
	Finished                  uint64 `json:"finished,string"`
	HTTPResponses             uint64 `json:"httpResponses,string"`
	HTTPErrors                uint64 `json:"httpErrors,string"`
	TransportErrors           uint64 `json:"transportErrors,string"`
	Timeouts                  uint64 `json:"timeouts,string"`
	Cancelled                 uint64 `json:"cancelled,string"`
	ResponseBodyLimitExceeded uint64 `json:"responseBodyLimitExceeded,string"`
}

// Histogram использует 20 некумулятивных корзин latency-us-v1 и отдельное переполнение.
type Histogram struct {
	LayoutID      string     `json:"layoutId"`
	BucketCounts  [20]uint64 `json:"bucketCounts"`
	OverflowCount uint64     `json:"overflowCount,string"`
}

var boundsUs = [...]int64{100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 2500000, 5000000, 10000000, 30000000, 60000000, 120000000, 300000000}

// record округляет длительность вверх до микросекунды и выбирает включительную верхнюю границу.
func (h *Histogram) record(elapsed time.Duration) {
	micros := int64((elapsed + time.Microsecond - 1) / time.Microsecond)
	for index, bound := range boundsUs {
		if micros <= bound {
			h.BucketCounts[index]++
			return
		}
	}
	h.OverflowCount++
}

// Interval содержит секундные дельты; границы UTC выводятся из монотонной сетки запуска.
type Interval struct {
	StartAt          time.Time      `json:"startAt"`
	EndAt            time.Time      `json:"endAt"`
	Counters         Counters       `json:"counters"`
	HTTPStatusCounts map[int]uint64 `json:"httpStatusCounts"`
	HTTPLatency      Histogram      `json:"httpLatency"`
	FailureLatency   Histogram      `json:"failureLatency"`
}

// newInterval создаёт независимые счётчики для следующего интервала.
func newInterval(start time.Time) Interval {
	return Interval{StartAt: start, HTTPStatusCounts: make(map[int]uint64), HTTPLatency: Histogram{LayoutID: "latency-us-v1"}, FailureLatency: Histogram{LayoutID: "latency-us-v1"}}
}

// Result возвращает итоговые счётчики и последний неполный интервал после остановки всех запросов.
type Result struct {
	PlannedRequests      uint64     `json:"plannedRequests,string"`
	Totals               Counters   `json:"totals"`
	ActualStartAt        *time.Time `json:"actualStartAt"`
	GenerationStoppedAt  time.Time  `json:"generationStoppedAt"`
	FinishedAt           time.Time  `json:"finishedAt"`
	GenerationDurationUs uint64     `json:"generationDurationUs,string"`
	Stopped              bool       `json:"stopped"`
	MeasurementsLost     bool       `json:"measurementsLost"`
	Final                Interval   `json:"final"`
}
