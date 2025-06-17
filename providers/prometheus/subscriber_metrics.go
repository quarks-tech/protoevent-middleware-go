package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"

	"github.com/quarks-tech/protoevent-middleware-go/interceptors/eventstart"
)

type SubscriberMetrics struct {
	subscriberStartedCounter   *prometheus.CounterVec
	subscriberHandledCounter   *prometheus.CounterVec
	subscriberHandledHistogram *prometheus.HistogramVec
}

func NewSubscriberMetrics(opts ...SubscriberMetricsOption) *SubscriberMetrics {
	var config subscriberMetricsConfig
	config.apply(opts)

	m := &SubscriberMetrics{
		subscriberStartedCounter: prometheus.NewCounterVec(
			config.counterOpts.apply(prometheus.CounterOpts{
				Name: "event_subscriber_started_total",
				Help: "Total number of events started on the subscriber.",
			}), []string{"event_queue", "event_name"}),
		subscriberHandledCounter: prometheus.NewCounterVec(
			config.counterOpts.apply(prometheus.CounterOpts{
				Name: "event_subscriber_handled_total",
				Help: "Total number of events completed on the subscriber, regardless of success or failure.",
			}), []string{"event_queue", "event_name", "event_code"}),
	}

	if config.enableHandlingTimeHistogram {
		m.subscriberHandledHistogram = prometheus.NewHistogramVec(
			config.histogramOpts.apply(prometheus.HistogramOpts{
				Name:    "event_subscriber_handling_seconds",
				Help:    "Histogram of response latency (seconds) of event processing.",
				Buckets: DefaultHistogramBuckets,
			}), []string{"event_queue", "event_name"})
	}

	return m
}

func (m *SubscriberMetrics) Describe(ch chan<- *prometheus.Desc) {
	m.subscriberStartedCounter.Describe(ch)
	m.subscriberHandledCounter.Describe(ch)
	if m.subscriberHandledHistogram != nil {
		m.subscriberHandledHistogram.Describe(ch)
	}
}

func (m *SubscriberMetrics) Collect(ch chan<- prometheus.Metric) {
	m.subscriberStartedCounter.Collect(ch)
	m.subscriberHandledCounter.Collect(ch)
	if m.subscriberHandledHistogram != nil {
		m.subscriberHandledHistogram.Collect(ch)
	}
}

func (m *SubscriberMetrics) SubscriberInterceptor(opts ...Option) eventbus.SubscriberInterceptor {
	var options options
	options.apply(opts)

	return func(ctx context.Context, md *event.Metadata, e any, handler eventbus.Handler) error {
		eventQueue := options.getEventQueue(ctx)
		_, eventName := extractEventInfo(md.Type, e)
		m.subscriberStartedCounter.WithLabelValues(eventQueue, eventName).Inc()
		var startTime time.Time
		if m.subscriberHandledHistogram != nil {
			startTime = time.Now()
		}
		ctx = eventstart.WithContext(ctx, time.Now())
		err := handler(ctx, e)
		status := getEventStatus(err)
		m.subscriberHandledCounter.WithLabelValues(eventQueue, eventName, status).Inc()
		if m.subscriberHandledHistogram != nil {
			duration := time.Since(startTime).Seconds()
			observer := m.subscriberHandledHistogram.WithLabelValues(eventQueue, eventName)
			if options.exemplarFromContext != nil {
				if exemplar := options.exemplarFromContext(ctx); exemplar != nil {
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

func (m *SubscriberMetrics) MustRegister(registry prometheus.Registerer) {
	if err := m.Register(registry); err != nil {
		panic(err)
	}
}

func (m *SubscriberMetrics) Register(registry prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		m.subscriberStartedCounter,
		m.subscriberHandledCounter,
	}

	if m.subscriberHandledHistogram != nil {
		collectors = append(collectors, m.subscriberHandledHistogram)
	}

	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			return err
		}
	}

	return nil
}
