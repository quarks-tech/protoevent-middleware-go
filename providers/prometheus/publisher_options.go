package prometheus

type PublisherMetricsOption func(*publisherMetricsConfig)

func WithPublisherCounterOptions(opts ...CounterOption) PublisherMetricsOption {
	return func(c *publisherMetricsConfig) {
		c.counterOpts = opts
	}
}

func WithPublisherHandlingTimeHistogram(opts ...HistogramOption) PublisherMetricsOption {
	return func(c *publisherMetricsConfig) {
		c.enableHandlingTimeHistogram = true
		c.histogramOpts = opts
	}
}

type publisherMetricsConfig struct {
	counterOpts                 counterOpts
	histogramOpts               histogramOpts
	enableHandlingTimeHistogram bool
}

func (c *publisherMetricsConfig) apply(opts []PublisherMetricsOption) {
	for _, opt := range opts {
		opt(c)
	}
}
