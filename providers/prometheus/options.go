package prometheus

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

type CounterOption func(*prometheus.CounterOpts)

func WithCounterSubsystem(subsystem string) CounterOption {
	return func(o *prometheus.CounterOpts) {
		o.Subsystem = subsystem
	}
}

func WithCounterNamespace(namespace string) CounterOption {
	return func(o *prometheus.CounterOpts) {
		o.Namespace = namespace
	}
}

func WithCounterConstLabels(labels prometheus.Labels) CounterOption {
	return func(o *prometheus.CounterOpts) {
		o.ConstLabels = labels
	}
}

type HistogramOption func(*prometheus.HistogramOpts)

func WithHistogramBuckets(buckets []float64) HistogramOption {
	return func(o *prometheus.HistogramOpts) {
		o.Buckets = buckets
	}
}

func WithHistogramSubsystem(subsystem string) HistogramOption {
	return func(o *prometheus.HistogramOpts) {
		o.Subsystem = subsystem
	}
}

func WithHistogramNamespace(namespace string) HistogramOption {
	return func(o *prometheus.HistogramOpts) {
		o.Namespace = namespace
	}
}

func WithHistogramConstLabels(labels prometheus.Labels) HistogramOption {
	return func(o *prometheus.HistogramOpts) {
		o.ConstLabels = labels
	}
}

func (opts counterOpts) apply(base prometheus.CounterOpts) prometheus.CounterOpts {
	for _, opt := range opts {
		opt(&base)
	}
	return base
}

func (opts histogramOpts) apply(base prometheus.HistogramOpts) prometheus.HistogramOpts {
	for _, opt := range opts {
		opt(&base)
	}
	return base
}

type (
	counterOpts   []CounterOption
	histogramOpts []HistogramOption
	Option        func(*options)
)

func WithExemplarFromContext(f func(ctx context.Context) prometheus.Labels) Option {
	return func(o *options) {
		o.exemplarFromContext = f
	}
}

func WithEventQueue(queue string) Option {
	return func(o *options) {
		o.eventQueue = queue
	}
}

type options struct {
	exemplarFromContext func(ctx context.Context) prometheus.Labels
	eventQueue          string
}

func (o *options) apply(opts []Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func (o *options) getEventQueue(ctx context.Context) string {
	if o.eventQueue != "" {
		return o.eventQueue
	}

	return "unknown"
}
