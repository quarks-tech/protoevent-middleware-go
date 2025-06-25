package logging

import (
	"context"
	"time"

	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

var Now = time.Now

type Logger interface {
	WithField(key string, value any) Logger
	WithFields(fields map[string]any) Logger
	Errorf(format string, args ...any)
}

func SubscriberInterceptor(entry Logger) eventbus.SubscriberInterceptor {
	return func(ctx context.Context, md *event.Metadata, e any, handler eventbus.Handler) error {
		start := Now()

		ctx = WithContext(ctx, entry.WithFields(map[string]any{
			"event_id":                md.ID,
			"event_source":            md.Source,
			"event_type":              md.Type,
			"event_data_content_type": md.DataContentType,
		}))

		hErr := handler(ctx, e)
		if hErr == nil {
			return nil
		}

		FromContext(ctx).
			WithField("total_time", time.Since(md.Time).String()).
			WithField("processing_time", time.Since(start).String()).
			Errorf("error while handling event: %s %+v", hErr.Error(), e)

		return hErr
	}
}
