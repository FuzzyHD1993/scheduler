// Package metrics owns the Prometheus registry and the scheduler
// specific collectors.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles the scheduler-specific Prometheus metrics and the
// registry they are registered on.
type Metrics struct {
	registry *prometheus.Registry

	processed *prometheus.CounterVec
	failed    prometheus.Counter
	pending   prometheus.Gauge
}

// New creates a fresh registry (including Go and process collectors)
// with all scheduler metrics registered exactly once.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		prometheus.NewGoCollector(),
	)

	m := &Metrics{
		registry: reg,
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_processed_total",
			Help: "Number of jobs finished by workers, partitioned by final status.",
		}, []string{"status"}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_failed_total",
			Help: "Number of job executions that ended with an error (regardless of final status).",
		}),
		pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "jobs_pending",
			Help: "Number of jobs currently in pending state.",
		}),
	}
	reg.MustRegister(m.processed, m.failed, m.pending)
	return m
}

// IncProcessed increments jobs_processed_total for the given status.
func (m *Metrics) IncProcessed(status string) {
	m.processed.WithLabelValues(status).Inc()
}

// IncFailed increments jobs_failed_total.
func (m *Metrics) IncFailed() {
	m.failed.Inc()
}

// SetPending updates the jobs_pending gauge.
func (m *Metrics) SetPending(v float64) {
	m.pending.Set(v)
}

// Registry returns the metrics registry.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// Handler returns an http.Handler exposing this registry in the
// Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
