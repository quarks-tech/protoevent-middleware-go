package prometheus

import (
	"context"
	"errors"

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

func extractEventName(eventName string) string {
	if eventName == "" {
		return "unknown"
	}

	lastDotIndex := len(eventName) - 1
	for i := len(eventName) - 1; i >= 0; i-- {
		if eventName[i] == '.' {
			lastDotIndex = i
			break
		}
	}

	if lastDotIndex < len(eventName)-1 {
		return eventName[lastDotIndex+1:]
	}

	return eventName
}
