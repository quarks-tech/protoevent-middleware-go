package prometheus

import (
	"context"
	"errors"
	"strings"

	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

var DefaultHistogramBuckets = []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}

const (
	StatusOK            = "OK"
	StatusError         = "ERROR"
	StatusUnprocessable = "UNPROCESSABLE"
	StatusCancelled     = "CANCELLED"
	StatusTimeout       = "TIMEOUT"
)

func matchEventStatus(err error) string {
	switch {
	case err == nil:
		return StatusOK
	case eventbus.IsUnprocessableEventError(err):
		return StatusUnprocessable
	case errors.Is(err, context.Canceled):
		return StatusCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return StatusTimeout
	default:
		return StatusError
	}
}

func extractEventInfo(fullEventName string) (eventExchange, eventName string) {
	if fullEventName == "" {
		return "unknown", "unknown"
	}

	lastDotIndex := strings.LastIndex(fullEventName, ".")

	if lastDotIndex != -1 && lastDotIndex < len(fullEventName)-1 {
		return fullEventName[:lastDotIndex], fullEventName[lastDotIndex+1:]
	}

	return "unknown", fullEventName
}
