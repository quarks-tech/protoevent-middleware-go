package prometheus

import (
	"context"
	"errors"
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
	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("error.test.queue", "BookDeleted", "ERROR"))
	if counter != 1 {
		t.Errorf("Expected error counter to be 1, got %f", counter)
	}
}

func TestExtractEventInfo(t *testing.T) {
	tests := []struct {
		name              string
		eventName         string
		expectedQueue     string
		expectedEventName string
	}{
		{
			name:          "full event name",
			eventName:     "example.books.v1.BookCreated",
			expectedQueue: "unknown", expectedEventName: "BookCreated",
		},
		{
			name:              "simple event name",
			eventName:         "UserCreated",
			expectedQueue:     "unknown",
			expectedEventName: "UserCreated",
		},
		{
			name:              "empty event name",
			eventName:         "",
			expectedQueue:     "unknown",
			expectedEventName: "unknown",
		},
		{
			name:              "service with event",
			eventName:         "auth.UserLoggedIn",
			expectedQueue:     "unknown",
			expectedEventName: "UserLoggedIn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventName := extractEventName(tt.eventName)
			if eventName != tt.expectedEventName {
				t.Errorf("Expected event name %s, got %s", tt.expectedEventName, eventName)
			}
		})
	}
}

func TestExtractPublisherEventInfo(t *testing.T) {
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
			exchange, eventName := extractPublisherEventInfo(tt.fullEventName, nil)
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

	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("unprocessable.test.queue", "BookDeleted", "UNPROCESSABLE"))
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

	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("cancelled.test.queue", "BookDeleted", "CANCELLED"))
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

	counter := testutil.ToFloat64(subMetrics.subscriberHandledCounter.WithLabelValues("timeout.test.queue", "BookDeleted", "TIMEOUT"))
	if counter != 1 {
		t.Errorf("Expected timeout counter to be 1, got %f", counter)
	}
}
