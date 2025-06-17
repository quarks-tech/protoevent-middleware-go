package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

type PublisherMetrics struct {
	publisherStartedCounter   *prometheus.CounterVec
	publisherHandledCounter   *prometheus.CounterVec
	publisherHandledHistogram *prometheus.HistogramVec
}

func NewPublisherMetrics(opts ...PublisherMetricsOption) *PublisherMetrics {
	var config publisherMetricsConfig
	config.apply(opts)

	m := &PublisherMetrics{
		publisherStartedCounter: prometheus.NewCounterVec(
			config.counterOpts.apply(prometheus.CounterOpts{
				Name: "event_publisher_started_total",
				Help: "Total number of events started on the publisher.",
			}), []string{"event_exchange", "event_name"}),
		publisherHandledCounter: prometheus.NewCounterVec(
			config.counterOpts.apply(prometheus.CounterOpts{
				Name: "event_publisher_handled_total",
				Help: "Total number of events completed on the publisher, regardless of success or failure.",
			}), []string{"event_exchange", "event_name", "event_code"}),
	}

	if config.enableHandlingTimeHistogram {
		m.publisherHandledHistogram = prometheus.NewHistogramVec(
			config.histogramOpts.apply(prometheus.HistogramOpts{
				Name:    "event_publisher_handling_seconds",
				Help:    "Histogram of response latency (seconds) of event publishing.",
				Buckets: DefaultHistogramBuckets,
			}), []string{"event_exchange", "event_name"})
	}

	return m
}

func (m *PublisherMetrics) Describe(ch chan<- *prometheus.Desc) {
	m.publisherStartedCounter.Describe(ch)
	m.publisherHandledCounter.Describe(ch)
	if m.publisherHandledHistogram != nil {
		m.publisherHandledHistogram.Describe(ch)
	}
}

func (m *PublisherMetrics) Collect(ch chan<- prometheus.Metric) {
	m.publisherStartedCounter.Collect(ch)
	m.publisherHandledCounter.Collect(ch)
	if m.publisherHandledHistogram != nil {
		m.publisherHandledHistogram.Collect(ch)
	}
}

func (m *PublisherMetrics) PublisherInterceptor(opts ...Option) eventbus.PublisherInterceptor {
	var o options
	o.apply(opts)

	return func(ctx context.Context, name string, e any, p *eventbus.PublisherImpl, pf eventbus.PublishFn, publishOpts ...eventbus.PublishOption) error {
		eventExchange, eventName := extractPublisherEventInfo(name, e)
		m.publisherStartedCounter.WithLabelValues(eventExchange, eventName).Inc()
		var startTime time.Time
		if m.publisherHandledHistogram != nil {
			startTime = time.Now()
		}
		err := pf(ctx, name, e, p, publishOpts...)
		status := getEventStatus(err)
		m.publisherHandledCounter.WithLabelValues(eventExchange, eventName, status).Inc()
		if m.publisherHandledHistogram != nil {
			duration := time.Since(startTime).Seconds()
			observer := m.publisherHandledHistogram.WithLabelValues(eventExchange, eventName)
			if o.exemplarFromContext != nil {
				if exemplar := o.exemplarFromContext(ctx); exemplar != nil {
					observer.(prometheus.ExemplarObserver).ObserveWithExemplar(duration, exemplar)
				} else {
					observer.Observe(duration)
				}
			} else {
				observer.Observe(duration)
			}
		}

		return err
	}
}

func extractPublisherEventInfo(fullEventName string, event any) (eventExchange, eventName string) {
	if fullEventName == "" {
		return "unknown", "unknown"
	}
	lastDotIndex := -1
	for i := len(fullEventName) - 1; i >= 0; i-- {
		if fullEventName[i] == '.' {
			lastDotIndex = i
			break
		}
	}

	if lastDotIndex != -1 && lastDotIndex < len(fullEventName)-1 {
		eventExchange = fullEventName[:lastDotIndex]
		eventName = fullEventName[lastDotIndex+1:]
	} else {
		eventExchange = "unknown"
		eventName = fullEventName
	}

	return eventExchange, eventName
}

func (m *PublisherMetrics) MustRegister(registry prometheus.Registerer) {
	if err := m.Register(registry); err != nil {
		panic(err)
	}
}

func (m *PublisherMetrics) Register(registry prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		m.publisherStartedCounter,
		m.publisherHandledCounter,
	}

	if m.publisherHandledHistogram != nil {
		collectors = append(collectors, m.publisherHandledHistogram)
	}

	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			return err
		}
	}

	return nil
}
