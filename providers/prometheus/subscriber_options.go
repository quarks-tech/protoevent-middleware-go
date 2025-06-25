package prometheus

type SubscriberMetricsOption func(*subscriberMetricsConfig)

func WithSubscriberCounterOptions(opts ...CounterOption) SubscriberMetricsOption {
	return func(c *subscriberMetricsConfig) {
		c.counterOpts = opts
	}
}

func WithSubscriberHandlingTimeHistogram(opts ...HistogramOption) SubscriberMetricsOption {
	return func(c *subscriberMetricsConfig) {
		c.enableHandlingTimeHistogram = true
		c.histogramOpts = opts
	}
}

type subscriberMetricsConfig struct {
	counterOpts                 counterOpts
	histogramOpts               histogramOpts
	enableHandlingTimeHistogram bool
}

func (c *subscriberMetricsConfig) apply(opts []SubscriberMetricsOption) {
	for _, opt := range opts {
		opt(c)
	}
}
