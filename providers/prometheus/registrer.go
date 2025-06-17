package prometheus

import "github.com/prometheus/client_golang/prometheus"

type Metrics interface {
	prometheus.Collector
	Register(registry prometheus.Registerer) error
	MustRegister(registry prometheus.Registerer)
}

var (
	_ Metrics = (*PublisherMetrics)(nil)
	_ Metrics = (*SubscriberMetrics)(nil)
)

func registerCollectors(registry prometheus.Registerer, collectors ...prometheus.Collector) error {
	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			return err
		}
	}
	return nil
}
