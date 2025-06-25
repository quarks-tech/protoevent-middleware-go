package prometheus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

func TestSubscriberMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	subMetrics := NewSubscriberMetrics(
		WithSubscriberHandlingTimeHistogram(
			WithHistogramBuckets([]float64{0.1, 0.5, 1.0, 2.0, 5.0}),
		),
	)
	err := subMetrics.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register subscriber metrics: %v", err)
	}
	interceptor := subMetrics.SubscriberInterceptor(
		WithEventQueue("test.consumer.queue"),
	)
	md := &event.Metadata{
		Type: "example.books.v1.BookCreated",
		ID:   "test-123",
	}
	handler := func(ctx context.Context, e any) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}
	ctx := context.Background()
	err = interceptor(ctx, md, "test-event-data", handler)
	if err != nil {
		t.Fatalf("Interceptor failed: %v", err)
	}
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	if len(metricFamilies) == 0 {
		t.Fatal("No metrics were recorded")
	}
	found := false
	for _, mf := range metricFamilies {
		if *mf.Name == "event_subscriber_started_total" {
			found = true
			if len(mf.Metric) == 0 {
				t.Error("Expected at least one metric sample")
			}
		}
	}
	if !found {
		t.Error("Expected event_subscriber_started_total metric not found")
	}
}

func TestPublisherMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	pubMetrics := NewPublisherMetrics(
		WithPublisherHandlingTimeHistogram(
			WithHistogramBuckets([]float64{0.1, 0.5, 1.0, 2.0, 5.0}),
		),
	)
	err := pubMetrics.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register publisher metrics: %v", err)
	}
	interceptor := pubMetrics.PublisherInterceptor()
	mockPublisher := &eventbus.PublisherImpl{}
	mockPublishFn := func(ctx context.Context, name string, e any, p *eventbus.PublisherImpl, opts ...eventbus.PublishOption) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	}
	ctx := context.Background()
	err = interceptor(ctx, "example.books.v1.BookCreated", "test-event-data", mockPublisher, mockPublishFn)
	if err != nil {
		t.Fatalf("Interceptor failed: %v", err)
	}
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	if len(metricFamilies) == 0 {
		t.Fatal("No metrics were recorded")
	}
	found := false
	for _, mf := range metricFamilies {
		if *mf.Name == "event_publisher_started_total" {
			found = true
			if len(mf.Metric) == 0 {
				t.Error("Expected at least one metric sample")
			}
		}
	}
	if !found {
		t.Error("Expected event_publisher_started_total metric not found")
	}
}

func TestSubscriberMetricsWithError(t *testing.T) {
	registry := prometheus.NewRegistry()
	subMetrics := NewSubscriberMetrics()
	subMetrics.MustRegister(registry)

	interceptor := subMetrics.SubscriberInterceptor(
		WithEventQueue("error.test.queue"),
	)

	md := &event.Metadata{
		Type: "example.books.v1.BookDeleted",
		ID:   "test-error-123",
	}
	handler := func(ctx context.Context, e any) error {
		return errors.New("test error")
	}

	ctx := context.Background()
	err := interceptor(ctx, md, "test-event-data", handler)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}
	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("error.test.queue", "example.books.v1.BookDeleted", "ERROR"))
	if counter != 1 {
		t.Errorf("Expected error counter to be 1, got %f", counter)
	}
}

func TestExtractEventInfo(t *testing.T) {
	tests := []struct {
		name              string
		fullEventName     string
		expectedExchange  string
		expectedEventName string
	}{
		{
			name:              "full service event name",
			fullEventName:     "example.books.v1.BookCreated",
			expectedExchange:  "example.books.v1",
			expectedEventName: "BookCreated",
		},
		{
			name:              "simple service event",
			fullEventName:     "auth.UserLogin",
			expectedExchange:  "auth",
			expectedEventName: "UserLogin",
		},
		{
			name:              "single word event",
			fullEventName:     "NotificationSent",
			expectedExchange:  "unknown",
			expectedEventName: "NotificationSent",
		},
		{
			name:              "empty event name",
			fullEventName:     "",
			expectedExchange:  "unknown",
			expectedEventName: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exchange, eventName := extractEventInfo(tt.fullEventName)
			if exchange != tt.expectedExchange {
				t.Errorf("Expected exchange %s, got %s", tt.expectedExchange, exchange)
			}
			if eventName != tt.expectedEventName {
				t.Errorf("Expected event name %s, got %s", tt.expectedEventName, eventName)
			}
		})
	}
}

func TestGetEventStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "no error",
			err:      nil,
			expected: StatusOK,
		},
		{
			name:     "generic error",
			err:      errors.New("test error"),
			expected: StatusError,
		},
		{
			name:     "unprocessable event error",
			err:      eventbus.NewUnprocessableEventError(errors.New("invalid format")),
			expected: StatusUnprocessable,
		},
		{
			name:     "context cancelled",
			err:      context.Canceled,
			expected: StatusCancelled,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: StatusTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := matchEventStatus(tt.err)
			if status != tt.expected {
				t.Errorf("Expected status %s, got %s", tt.expected, status)
			}
		})
	}
}

func TestSubscriberMetricsWithUnprocessableError(t *testing.T) {
	registry := prometheus.NewRegistry()
	subMetrics := NewSubscriberMetrics()
	subMetrics.MustRegister(registry)

	interceptor := subMetrics.SubscriberInterceptor(
		WithEventQueue("unprocessable.test.queue"),
	)

	md := &event.Metadata{
		Type: "example.books.v1.BookDeleted",
		ID:   "test-unprocessable-123",
	}

	handler := func(ctx context.Context, e interface{}) error {
		return eventbus.NewUnprocessableEventError(errors.New("invalid event format"))
	}

	ctx := context.Background()
	err := interceptor(ctx, md, "test-event-data", handler)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("unprocessable.test.queue", "example.books.v1.BookDeleted", "UNPROCESSABLE"))
	if counter != 1 {
		t.Errorf("Expected unprocessable counter to be 1, got %f", counter)
	}
}

func TestSubscriberMetricsWithCancellation(t *testing.T) {
	registry := prometheus.NewRegistry()
	subMetrics := NewSubscriberMetrics()
	subMetrics.MustRegister(registry)

	interceptor := subMetrics.SubscriberInterceptor(
		WithEventQueue("cancelled.test.queue"),
	)

	md := &event.Metadata{
		Type: "example.books.v1.BookDeleted",
		ID:   "test-cancelled-123",
	}

	handler := func(ctx context.Context, e interface{}) error {
		return context.Canceled
	}

	ctx := context.Background()
	err := interceptor(ctx, md, "test-event-data", handler)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("cancelled.test.queue", "example.books.v1.BookDeleted", "CANCELLED"))
	if counter != 1 {
		t.Errorf("Expected cancelled counter to be 1, got %f", counter)
	}
}

func TestSubscriberMetricsWithTimeout(t *testing.T) {
	registry := prometheus.NewRegistry()
	subMetrics := NewSubscriberMetrics()
	subMetrics.MustRegister(registry)

	interceptor := subMetrics.SubscriberInterceptor(
		WithEventQueue("timeout.test.queue"),
	)

	md := &event.Metadata{
		Type: "example.books.v1.BookDeleted",
		ID:   "test-timeout-123",
	}

	handler := func(ctx context.Context, e interface{}) error {
		return context.DeadlineExceeded
	}

	ctx := context.Background()
	err := interceptor(ctx, md, "test-event-data", handler)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("timeout.test.queue", "example.books.v1.BookDeleted", "TIMEOUT"))
	if counter != 1 {
		t.Errorf("Expected timeout counter to be 1, got %f", counter)
	}
}

// Test Configuration Options
func TestPublisherMetricsWithCustomOptions(t *testing.T) {
	registry := prometheus.NewRegistry()

	pubMetrics := NewPublisherMetrics(
		WithPublisherCounterOptions(
			WithCounterNamespace("custom"),
			WithCounterSubsystem("events"),
			WithCounterConstLabels(prometheus.Labels{"service": "test"}),
		),
		WithPublisherHandlingTimeHistogram(
			WithHistogramNamespace("custom"),
			WithHistogramSubsystem("events"),
			WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 1.0}),
			WithHistogramConstLabels(prometheus.Labels{"service": "test"}),
		),
	)

	err := pubMetrics.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register publisher metrics: %v", err)
	}

	// Trigger metrics by using the interceptor
	interceptor := pubMetrics.PublisherInterceptor()
	mockPublisher := &eventbus.PublisherImpl{}
	mockPublishFn := func(ctx context.Context, name string, e any, p *eventbus.PublisherImpl, opts ...eventbus.PublishOption) error {
		time.Sleep(time.Millisecond)
		return nil
	}

	ctx := context.Background()
	err = interceptor(ctx, "test.service.v1.TestEvent", "test-data", mockPublisher, mockPublishFn)
	if err != nil {
		t.Fatalf("Interceptor failed: %v", err)
	}

	// Check that metrics have correct names
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	expectedMetrics := []string{
		"custom_events_event_publisher_started_total",
		"custom_events_event_publisher_handled_total",
		"custom_events_event_publisher_handling_seconds",
	}

	foundMetrics := make(map[string]bool)
	for _, mf := range metricFamilies {
		foundMetrics[*mf.Name] = true
	}

	for _, expected := range expectedMetrics {
		if !foundMetrics[expected] {
			t.Errorf("Expected metric %s not found", expected)
		}
	}
}

func TestSubscriberMetricsWithCustomOptions(t *testing.T) {
	registry := prometheus.NewRegistry()

	subMetrics := NewSubscriberMetrics(
		WithSubscriberCounterOptions(
			WithCounterNamespace("myapp"),
			WithCounterSubsystem("subscriber"),
		),
		WithSubscriberHandlingTimeHistogram(
			WithHistogramBuckets([]float64{0.5, 1.0, 2.0}),
		),
	)

	err := subMetrics.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register subscriber metrics: %v", err)
	}

	// Trigger metrics by using the interceptor
	interceptor := subMetrics.SubscriberInterceptor(
		WithEventQueue("test.queue"),
	)

	md := &event.Metadata{
		Type: "test.service.v1.TestEvent",
		ID:   "test-123",
	}

	handler := func(ctx context.Context, e any) error {
		time.Sleep(time.Millisecond)
		return nil
	}

	ctx := context.Background()
	err = interceptor(ctx, md, "test-event-data", handler)
	if err != nil {
		t.Fatalf("Interceptor failed: %v", err)
	}

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	expectedMetrics := []string{
		"myapp_subscriber_event_subscriber_started_total",
		"myapp_subscriber_event_subscriber_handled_total",
		"event_subscriber_handling_seconds", // No custom namespace/subsystem for histogram
	}

	foundMetrics := make(map[string]bool)
	for _, mf := range metricFamilies {
		foundMetrics[*mf.Name] = true
	}

	for _, expected := range expectedMetrics {
		if !foundMetrics[expected] {
			t.Errorf("Expected metric %s not found", expected)
		}
	}
}

// Test Exemplar Functionality
func TestPublisherMetricsWithExemplars(t *testing.T) {
	registry := prometheus.NewRegistry()
	pubMetrics := NewPublisherMetrics(
		WithPublisherHandlingTimeHistogram(),
	)
	err := pubMetrics.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register publisher metrics: %v", err)
	}

	exemplarFromContext := func(ctx context.Context) prometheus.Labels {
		if traceID := ctx.Value("traceID"); traceID != nil {
			return prometheus.Labels{"traceID": traceID.(string)}
		}
		return nil
	}

	interceptor := pubMetrics.PublisherInterceptor(
		WithExemplarFromContext(exemplarFromContext),
	)

	mockPublisher := &eventbus.PublisherImpl{}
	mockPublishFn := func(ctx context.Context, name string, e any, p *eventbus.PublisherImpl, opts ...eventbus.PublishOption) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	}

	// Test with trace context
	ctx := context.WithValue(context.Background(), "traceID", "trace-123")
	err = interceptor(ctx, "user.service.v1.UserCreated", "test-event", mockPublisher, mockPublishFn)
	if err != nil {
		t.Fatalf("Interceptor failed: %v", err)
	}

	// Verify histogram was recorded
	histogram := pubMetrics.publisherHandledHistogram.WithLabelValues("user.service.v1", "UserCreated")
	if histogram == nil {
		t.Fatal("Expected histogram to be created")
	}
}

func TestSubscriberMetricsWithExemplars(t *testing.T) {
	registry := prometheus.NewRegistry()
	subMetrics := NewSubscriberMetrics(
		WithSubscriberHandlingTimeHistogram(),
	)
	err := subMetrics.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register subscriber metrics: %v", err)
	}

	exemplarFromContext := func(ctx context.Context) prometheus.Labels {
		if spanID := ctx.Value("spanID"); spanID != nil {
			return prometheus.Labels{"spanID": spanID.(string)}
		}
		return nil
	}

	interceptor := subMetrics.SubscriberInterceptor(
		WithEventQueue("user.consumers.v1"),
		WithExemplarFromContext(exemplarFromContext),
	)

	md := &event.Metadata{
		Type: "user.service.v1.UserCreated",
		ID:   "test-123",
	}

	handler := func(ctx context.Context, e any) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	// Test with span context
	ctx := context.WithValue(context.Background(), "spanID", "span-456")
	err = interceptor(ctx, md, "test-event", handler)
	if err != nil {
		t.Fatalf("Interceptor failed: %v", err)
	}

	// Verify metrics were recorded
	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("user.consumers.v1", "user.service.v1.UserCreated", "OK"))
	if counter != 1 {
		t.Errorf("Expected counter to be 1, got %f", counter)
	}
}

// Test Edge Cases
func TestExtractEventInfoEdgeCases(t *testing.T) {
	tests := []struct {
		name              string
		input             string
		expectedExchange  string
		expectedEventName string
	}{
		{
			name:              "empty string",
			input:             "",
			expectedExchange:  "unknown",
			expectedEventName: "unknown",
		},
		{
			name:              "single dot",
			input:             ".",
			expectedExchange:  "unknown",
			expectedEventName: ".",
		},
		{
			name:              "starts with dot",
			input:             ".EventName",
			expectedExchange:  "",
			expectedEventName: "EventName",
		},
		{
			name:              "ends with dot",
			input:             "service.name.",
			expectedExchange:  "unknown",
			expectedEventName: "service.name.",
		},
		{
			name:              "multiple consecutive dots",
			input:             "service..name...Event",
			expectedExchange:  "service..name..",
			expectedEventName: "Event",
		},
		{
			name:              "very long event name",
			input:             "very.long.service.name.with.many.segments.v1.VeryLongEventNameThatShouldStillWork",
			expectedExchange:  "very.long.service.name.with.many.segments.v1",
			expectedEventName: "VeryLongEventNameThatShouldStillWork",
		},
		{
			name:              "single character segments",
			input:             "a.b.c.d.E",
			expectedExchange:  "a.b.c.d",
			expectedEventName: "E",
		},
		{
			name:              "unicode characters",
			input:             "服务.名称.v1.事件创建",
			expectedExchange:  "服务.名称.v1",
			expectedEventName: "事件创建",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exchange, eventName := extractEventInfo(tt.input)
			if exchange != tt.expectedExchange {
				t.Errorf("Expected exchange %q, got %q", tt.expectedExchange, exchange)
			}
			if eventName != tt.expectedEventName {
				t.Errorf("Expected event name %q, got %q", tt.expectedEventName, eventName)
			}
		})
	}
}

// Test Concurrent Usage
func TestPublisherMetricsConcurrency(t *testing.T) {
	registry := prometheus.NewRegistry()
	pubMetrics := NewPublisherMetrics(
		WithPublisherHandlingTimeHistogram(),
	)
	err := pubMetrics.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register publisher metrics: %v", err)
	}

	interceptor := pubMetrics.PublisherInterceptor()

	const numGoroutines = 100
	const eventsPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			mockPublisher := &eventbus.PublisherImpl{}
			mockPublishFn := func(ctx context.Context, name string, e any, p *eventbus.PublisherImpl, opts ...eventbus.PublishOption) error {
				// Simulate some work
				time.Sleep(time.Microsecond * time.Duration(goroutineID%10))
				if goroutineID%20 == 0 {
					return errors.New("simulated error")
				}
				return nil
			}

			for j := 0; j < eventsPerGoroutine; j++ {
				ctx := context.Background()
				eventName := "test.service.v1.TestEvent"
				if j%3 == 0 {
					eventName = "test.service.v2.TestEvent"
				}

				_ = interceptor(ctx, eventName, "test-data", mockPublisher, mockPublishFn)
			}
		}(i)
	}

	wg.Wait()

	// Verify metrics were recorded correctly
	totalEvents := numGoroutines * eventsPerGoroutine

	// Check v1 events
	v1Started := testutil.ToFloat64(pubMetrics.publisherStartedCounter.WithLabelValues("test.service.v1", "TestEvent"))
	v1Handled := testutil.ToFloat64(pubMetrics.publisherHandledCounter.WithLabelValues("test.service.v1", "TestEvent", "OK")) +
		testutil.ToFloat64(pubMetrics.publisherHandledCounter.WithLabelValues("test.service.v1", "TestEvent", "ERROR"))

	// Check v2 events
	v2Started := testutil.ToFloat64(pubMetrics.publisherStartedCounter.WithLabelValues("test.service.v2", "TestEvent"))
	v2Handled := testutil.ToFloat64(pubMetrics.publisherHandledCounter.WithLabelValues("test.service.v2", "TestEvent", "OK")) +
		testutil.ToFloat64(pubMetrics.publisherHandledCounter.WithLabelValues("test.service.v2", "TestEvent", "ERROR"))

	if int(v1Started+v2Started) != totalEvents {
		t.Errorf("Expected %d total started events, got %f", totalEvents, v1Started+v2Started)
	}

	if int(v1Handled+v2Handled) != totalEvents {
		t.Errorf("Expected %d total handled events, got %f", totalEvents, v1Handled+v2Handled)
	}

	if v1Started != v1Handled {
		t.Errorf("V1 started (%f) should equal handled (%f)", v1Started, v1Handled)
	}

	if v2Started != v2Handled {
		t.Errorf("V2 started (%f) should equal handled (%f)", v2Started, v2Handled)
	}
}

// Test Registration Edge Cases
func TestDoubleRegistration(t *testing.T) {
	registry := prometheus.NewRegistry()
	pubMetrics := NewPublisherMetrics()

	// First registration should succeed
	err := pubMetrics.Register(registry)
	if err != nil {
		t.Fatalf("First registration failed: %v", err)
	}

	// Second registration should fail
	err = pubMetrics.Register(registry)
	if err == nil {
		t.Fatal("Expected second registration to fail")
	}
}

func TestMustRegisterPanic(t *testing.T) {
	registry := prometheus.NewRegistry()
	pubMetrics := NewPublisherMetrics()

	// First registration should succeed
	pubMetrics.MustRegister(registry)

	// Second registration should panic
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected MustRegister to panic on second registration")
		}
	}()

	pubMetrics.MustRegister(registry)
}

// Test Without Histogram
func TestMetricsWithoutHistogram(t *testing.T) {
	registry := prometheus.NewRegistry()
	pubMetrics := NewPublisherMetrics() // No histogram options
	err := pubMetrics.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register publisher metrics: %v", err)
	}

	interceptor := pubMetrics.PublisherInterceptor()

	mockPublisher := &eventbus.PublisherImpl{}
	mockPublishFn := func(ctx context.Context, name string, e any, p *eventbus.PublisherImpl, opts ...eventbus.PublishOption) error {
		return nil
	}

	ctx := context.Background()
	err = interceptor(ctx, "test.service.v1.TestEvent", "test-data", mockPublisher, mockPublishFn)
	if err != nil {
		t.Fatalf("Interceptor failed: %v", err)
	}

	// Should only have counters, no histogram
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	foundHistogram := false
	for _, mf := range metricFamilies {
		if *mf.Name == "event_publisher_handling_seconds" {
			foundHistogram = true
		}
	}

	if foundHistogram {
		t.Error("Should not have histogram when not configured")
	}
}

// Test Event Status Mapping Edge Cases
func TestEventStatusMappingWithWrappedErrors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "wrapped context canceled",
			err:      errors.Join(errors.New("operation failed"), context.Canceled),
			expected: StatusCancelled,
		},
		{
			name:     "wrapped deadline exceeded",
			err:      errors.Join(errors.New("timeout occurred"), context.DeadlineExceeded),
			expected: StatusTimeout,
		},
		{
			name:     "wrapped unprocessable error",
			err:      errors.Join(errors.New("validation failed"), eventbus.NewUnprocessableEventError(errors.New("invalid data"))),
			expected: StatusUnprocessable,
		},
		{
			name:     "multiple wrapped errors with context canceled",
			err:      errors.Join(errors.New("first"), errors.New("second"), context.Canceled),
			expected: StatusCancelled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := matchEventStatus(tt.err)
			if status != tt.expected {
				t.Errorf("Expected status %s, got %s", tt.expected, status)
			}
		})
	}
}

// Test Metrics Interface Compliance
func TestMetricsInterfaceCompliance(t *testing.T) {
	// Test that both metrics implement the Metrics interface
	var _ Metrics = (*PublisherMetrics)(nil)
	var _ Metrics = (*SubscriberMetrics)(nil)

	// Test Describe/Collect functionality
	pubMetrics := NewPublisherMetrics(WithPublisherHandlingTimeHistogram())
	subMetrics := NewSubscriberMetrics(WithSubscriberHandlingTimeHistogram())

	// Test Describe
	descCh := make(chan *prometheus.Desc, 10)
	go func() {
		pubMetrics.Describe(descCh)
		subMetrics.Describe(descCh)
		close(descCh)
	}()

	descCount := 0
	for range descCh {
		descCount++
	}

	if descCount == 0 {
		t.Error("Expected metrics to provide descriptions")
	}

	// Test Collect - need to trigger some metrics first
	registry := prometheus.NewRegistry()
	pubMetrics.Register(registry)
	subMetrics.Register(registry)

	// Trigger some metrics
	pubInterceptor := pubMetrics.PublisherInterceptor()
	mockPublisher := &eventbus.PublisherImpl{}
	mockPublishFn := func(ctx context.Context, name string, e any, p *eventbus.PublisherImpl, opts ...eventbus.PublishOption) error {
		return nil
	}
	_ = pubInterceptor(context.Background(), "test.service.v1.TestEvent", "test-data", mockPublisher, mockPublishFn)

	subInterceptor := subMetrics.SubscriberInterceptor(WithEventQueue("test.queue"))
	md := &event.Metadata{Type: "test.service.v1.TestEvent", ID: "test-123"}
	handler := func(ctx context.Context, e any) error { return nil }
	_ = subInterceptor(context.Background(), md, "test-event", handler)

	metricCh := make(chan prometheus.Metric, 20)
	go func() {
		pubMetrics.Collect(metricCh)
		subMetrics.Collect(metricCh)
		close(metricCh)
	}()

	metricCount := 0
	for range metricCh {
		metricCount++
	}

	if metricCount == 0 {
		t.Error("Expected metrics to provide metric samples")
	}
}

// Test Large-scale Event Processing
func TestLargeScaleEventProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large-scale test in short mode")
	}

	registry := prometheus.NewRegistry()
	subMetrics := NewSubscriberMetrics(
		WithSubscriberHandlingTimeHistogram(),
	)
	err := subMetrics.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register subscriber metrics: %v", err)
	}

	interceptor := subMetrics.SubscriberInterceptor(
		WithEventQueue("load.test.queue"),
	)

	const numEvents = 10000
	processed := 0

	handler := func(ctx context.Context, e any) error {
		processed++
		// Simulate processing time
		if processed%1000 == 0 {
			time.Sleep(time.Microsecond)
		}
		return nil
	}

	start := time.Now()

	for i := 0; i < numEvents; i++ {
		md := &event.Metadata{
			Type: "load.test.v1.TestEvent",
			ID:   "test-" + string(rune(i)),
		}

		err := interceptor(context.Background(), md, "test-data", handler)
		if err != nil {
			t.Fatalf("Event %d failed: %v", i, err)
		}
	}

	duration := time.Since(start)
	eventsPerSecond := float64(numEvents) / duration.Seconds()

	t.Logf("Processed %d events in %v (%.2f events/sec)", numEvents, duration, eventsPerSecond)

	// Verify all events were recorded
	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("load.test.queue", "load.test.v1.TestEvent", "OK"))
	if int(counter) != numEvents {
		t.Errorf("Expected %d events recorded, got %f", numEvents, counter)
	}
}

// Benchmark Tests
func BenchmarkPublisherInterceptor(b *testing.B) {
	pubMetrics := NewPublisherMetrics(WithPublisherHandlingTimeHistogram())
	interceptor := pubMetrics.PublisherInterceptor()

	mockPublisher := &eventbus.PublisherImpl{}
	mockPublishFn := func(ctx context.Context, name string, e any, p *eventbus.PublisherImpl, opts ...eventbus.PublishOption) error {
		return nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx := context.Background()
			_ = interceptor(ctx, "benchmark.service.v1.BenchmarkEvent", "test-data", mockPublisher, mockPublishFn)
		}
	})
}

func BenchmarkSubscriberInterceptor(b *testing.B) {
	subMetrics := NewSubscriberMetrics(WithSubscriberHandlingTimeHistogram())
	interceptor := subMetrics.SubscriberInterceptor(WithEventQueue("benchmark.queue"))

	handler := func(ctx context.Context, e any) error {
		return nil
	}

	md := &event.Metadata{
		Type: "benchmark.service.v1.BenchmarkEvent",
		ID:   "bench-123",
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx := context.Background()
			_ = interceptor(ctx, md, "test-data", handler)
		}
	})
}

func BenchmarkExtractEventInfo(b *testing.B) {
	eventNames := []string{
		"simple.Event",
		"service.name.v1.EventName",
		"very.long.service.name.with.many.segments.v2.VeryLongEventName",
		"a.b.c.d.e.f.g.h.i.j.Event",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eventName := eventNames[i%len(eventNames)]
		extractEventInfo(eventName)
	}
}
