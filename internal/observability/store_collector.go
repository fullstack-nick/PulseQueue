package observability

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

type storeSnapshotProvider interface {
	ObservabilitySnapshot(ctx context.Context) (storage.ObservabilitySnapshot, error)
}

type storeCollector struct {
	store         storeSnapshotProvider
	scrapeError   *prometheus.Desc
	jobsByStatus  *prometheus.Desc
	queueDepth    *prometheus.Desc
	activeJobs    *prometheus.Desc
	activeWorkers *prometheus.Desc
}

func newStoreCollector(store storeSnapshotProvider) *storeCollector {
	return &storeCollector{
		store: store,
		scrapeError: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "store_scrape_error"),
			"Whether the most recent PostgreSQL-backed observability scrape failed.",
			nil,
			nil,
		),
		jobsByStatus: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "jobs_by_status"),
			"Current jobs by queue and status from PostgreSQL.",
			[]string{"queue", "status"},
			nil,
		),
		queueDepth: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "queue_depth"),
			"Current queued and retry-scheduled jobs by queue from PostgreSQL.",
			[]string{"queue"},
			nil,
		),
		activeJobs: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "active_jobs"),
			"Current running jobs by queue from PostgreSQL.",
			[]string{"queue"},
			nil,
		),
		activeWorkers: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "active_workers"),
			"Current running workers by queue from PostgreSQL.",
			[]string{"queue"},
			nil,
		),
	}
}

func (c *storeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.scrapeError
	ch <- c.jobsByStatus
	ch <- c.queueDepth
	ch <- c.activeJobs
	ch <- c.activeWorkers
}

func (c *storeCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	snapshot, err := c.store.ObservabilitySnapshot(ctx)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.scrapeError, prometheus.GaugeValue, 1)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.scrapeError, prometheus.GaugeValue, 0)

	for _, metric := range snapshot.JobsByStatus {
		ch <- prometheus.MustNewConstMetric(c.jobsByStatus, prometheus.GaugeValue, float64(metric.Count), metric.Queue, metric.Status)
	}
	for _, metric := range snapshot.QueueDepth {
		ch <- prometheus.MustNewConstMetric(c.queueDepth, prometheus.GaugeValue, float64(metric.Value), metric.Queue)
	}
	for _, metric := range snapshot.ActiveJobs {
		ch <- prometheus.MustNewConstMetric(c.activeJobs, prometheus.GaugeValue, float64(metric.Value), metric.Queue)
	}
	for _, metric := range snapshot.ActiveWorkers {
		ch <- prometheus.MustNewConstMetric(c.activeWorkers, prometheus.GaugeValue, float64(metric.Value), metric.Queue)
	}
}
