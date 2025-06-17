package prometheus

var DefaultHistogramBuckets = []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}

const (
	StatusOK     = "OK"
	StatusError  = "ERROR"
	StatusPanic  = "PANIC"
	StatusCancel = "CANCELLED"
)

func getEventStatus(err error) string {
	if err == nil {
		return StatusOK
	}
	return StatusError
}

func extractEventInfo(eventName string, event any) (eventQueue, eventNameLabel string) {
	if eventName == "" {
		return "unknown", "unknown"
	}

	lastDotIndex := len(eventName) - 1
	for i := len(eventName) - 1; i >= 0; i-- {
		if eventName[i] == '.' {
			lastDotIndex = i

			break
		}
	}

	if lastDotIndex < len(eventName)-1 {
		eventNameLabel = eventName[lastDotIndex+1:]
	} else {
		eventNameLabel = eventName
	}

	eventQueue = "unknown"

	return eventQueue, eventNameLabel
}
