# Prometheus Provider for Protoevent Middleware

This package provides Prometheus metrics middleware for the protoevent event bus system, specifically designed for AMQP/RabbitMQ usage. It's inspired by the `grpc-ecosystem/go-grpc-middleware` prometheus provider but adapted for protoevent's event-driven architecture with AMQP transport.

## Features

- **Event Metrics**: Track event processing counts, latencies, and success/error rates
- **Publisher Metrics**: Monitor event publishing performance with exchange-based labels
- **Subscriber Metrics**: Monitor event processing performance with queue-based labels
- **AMQP-Optimized Labels**: Uses `event_queue`/`event_exchange` and `event_name` labels
- **Histogram Support**: Optional latency histograms with configurable buckets
- **Exemplar Support**: Integration with tracing systems (OpenTelemetry, Jaeger, etc.)
- **Configurable Labels**: Customize metric namespaces, subsystems, and constant labels

## Installation

```bash
go get github.com/quarks-tech/protoevent-middleware-go/providers/prometheus
```

## AMQP/RabbitMQ Context

This middleware is optimized for AMQP usage where:
- **Events** follow the pattern `service.name.version.EventName` (e.g., `user.service.v1.UserCreated`)
- **Publishers** send to **exchanges** (derived from service name: `user.service.v1`)
- **Subscribers** consume from **queues** (e.g., `user.service.consumers.v1`)
- **Event names** are extracted from the full event type (e.g., `UserCreated`)

## Usage

### Basic Subscriber Metrics with Queue Name

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/quarks-tech/protoevent-go/pkg/eventbus"
    eventprometheus "github.com/quarks-tech/protoevent-middleware-go/providers/prometheus"
)

reg := prometheus.NewRegistry()

subMetrics := eventprometheus.NewSubscriberMetrics()
reg.MustRegister(subMetrics)

subscriber := eventbus.NewSubscriber(
    transport,
    eventbus.WithSubscriberInterceptor(
        subMetrics.SubscriberInterceptor(
            eventprometheus.WithEventQueue("user.service.consumers.v1"),        ),
    ),
)
```

### Publisher Metrics (Exchange-based)

```go
pubMetrics := eventprometheus.NewPublisherMetrics(
    eventprometheus.WithPublisherHandlingTimeHistogram(),
)
reg.MustRegister(pubMetrics)

publisher := eventbus.NewPublisher(
    transport,
    eventbus.WithPublisherInterceptor(pubMetrics.PublisherInterceptor()),
)
```

### Exemplar Support (for Tracing Integration)

```go
import (
    "go.opentelemetry.io/otel/trace"
)

exemplarFromContext := func(ctx context.Context) prometheus.Labels {
    if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
        return prometheus.Labels{"traceID": span.TraceID().String()}
    }
    return nil
}

interceptor := subMetrics.SubscriberInterceptor(
    eventprometheus.WithEventQueue("user.service.consumers.v1"),
    eventprometheus.WithExemplarFromContext(exemplarFromContext),
)
```

## Available Metrics

### Subscriber Metrics

- `event_subscriber_started_total{event_queue, event_name}`: Total events started
- `event_subscriber_handled_total{event_queue, event_name, event_code}`: Total events completed
- `event_subscriber_handling_seconds{event_queue, event_name}`: Event processing latency histogram (optional)

### Publisher Metrics

- `event_publisher_started_total{event_exchange, event_name}`: Total events started
- `event_publisher_handled_total{event_exchange, event_name, event_code}`: Total events completed
- `event_publisher_handling_seconds{event_exchange, event_name}`: Event publishing latency histogram (optional)

### Labels Explained

**Subscriber Labels:**
- `event_queue`: AMQP consumer queue name (e.g., `user.service.consumers.v1`)
- `event_name`: Event name extracted from full type (e.g., `UserCreated` from `user.service.v1.UserCreated`)
- `event_code`: Status code (`OK`, `ERROR`, `PANIC`, `CANCELLED`)

**Publisher Labels:**
- `event_exchange`: AMQP exchange derived from service (e.g., `user.service.v1` from `user.service.v1.UserCreated`)
- `event_name`: Event name extracted from full type (e.g., `UserCreated`)
- `event_code`: Status code (`OK`, `ERROR`, `PANIC`, `CANCELLED`)

## Configuration Options

### Counter Options

- `WithCounterNamespace(namespace string)`: Set metric namespace
- `WithCounterSubsystem(subsystem string)`: Set metric subsystem
- `WithCounterConstLabels(labels prometheus.Labels)`: Set constant labels

### Histogram Options

- `WithHistogramBuckets(buckets []float64)`: Set custom histogram buckets
- `WithHistogramNamespace(namespace string)`: Set metric namespace
- `WithHistogramSubsystem(subsystem string)`: Set metric subsystem
- `WithHistogramConstLabels(labels prometheus.Labels)`: Set constant labels

### Interceptor Options

- `WithExemplarFromContext(func(ctx context.Context) prometheus.Labels)`: Extract exemplars from context
- `WithEventQueue(queue string)`: Set static queue name for subscriber metrics

## Example Queries

### Event Processing Rate by Queue
```promql
rate(event_subscriber_handled_total[5m])
```

### Error Rate by Queue
```promql
rate(event_subscriber_handled_total{event_code!="OK"}[5m]) / rate(event_subscriber_handled_total[5m])
```

### Publishing Rate by Exchange
```promql
rate(event_publisher_handled_total[5m])
```

### 95th Percentile Processing Latency by Event Type
```promql
histogram_quantile(0.95, rate(event_subscriber_handling_seconds_bucket[5m]))
```

### Events per Queue
```promql
sum(rate(event_subscriber_handled_total[5m])) by (event_queue)
```

### Events per Exchange
```promql
sum(rate(event_publisher_handled_total[5m])) by (event_exchange)
```

### Top Event Types by Volume
```promql
topk(10, sum(rate(event_subscriber_handled_total[5m])) by (event_name))
```

## AMQP Integration Example

```go
import (
    "github.com/quarks-tech/protoevent-amqp-go/pkg/rabbitmq"
    eventprometheus "github.com/quarks-tech/protoevent-middleware-go/providers/prometheus"
)

receiver := rabbitmq.NewReceiver(client,
    rabbitmq.WithIncomingQueue("user.service.consumers.v1"),
    rabbitmq.WithTopologySetup(),
)

subscriber := eventbus.NewSubscriber(
    "user.service.consumers.v1",    eventbus.WithSubscriberInterceptor(
        subMetrics.SubscriberInterceptor(
            eventprometheus.WithEventQueue("user.service.consumers.v1"),
        ),
    ),
)
```

## Sample Metrics Output

```prometheus
# Subscriber metrics
event_subscriber_started_total{event_queue="user.service.consumers.v1",event_name="UserCreated"} 42
event_subscriber_handled_total{event_queue="user.service.consumers.v1",event_name="UserCreated",event_code="OK"} 40
event_subscriber_handled_total{event_queue="user.service.consumers.v1",event_name="UserCreated",event_code="ERROR"} 2

# Publisher metrics  
event_publisher_started_total{event_exchange="user.service.v1",event_name="UserCreated"} 42
event_publisher_handled_total{event_exchange="user.service.v1",event_name="UserCreated",event_code="OK"} 42

# Latency histograms
event_subscriber_handling_seconds_bucket{event_queue="user.service.consumers.v1",event_name="UserCreated",le="0.1"} 35
event_subscriber_handling_seconds_bucket{event_queue="user.service.consumers.v1",event_name="UserCreated",le="0.5"} 40
event_subscriber_handling_seconds_bucket{event_queue="user.service.consumers.v1",event_name="UserCreated",le="+Inf"} 42
```

## Complete Example

See the [example/main.go](example/main.go) file for a complete working example that demonstrates:

- AMQP-style event naming (`service.name.version.EventName`)
- Publisher metrics with exchange labels
- Subscriber metrics with queue labels
- Multiple event types
- Error simulation
- Histogram configuration

To run the example:

```bash
cd example
go run main.go
```

Then visit `http:any

## Performance Considerations

- Histogram metrics can be expensive in high-throughput scenarios
- Consider using appropriate bucket configurations for your use case
- Use sampling for exemplars in high-volume environments
- Monitor the cardinality of your metrics (queue × event_name combinations)
- For high-cardinality scenarios, consider using constant labels or dropping specific event names

## License

Licensed under the Apache License 2.0. See LICENSE file for details.
