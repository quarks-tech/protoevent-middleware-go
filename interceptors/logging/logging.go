package logging

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

var Now = time.Now

func LoggingInterceptor(withContext WithContext, fromContext FromContext) eventbus.SubscriberInterceptor {
	return func(ctx context.Context, md *event.Metadata, e any, handler eventbus.Handler) error {
		start := Now()

		ctx = withContext(
			withContext(
				ctx,
				fromContext(ctx),
			),
			logrus.WithField("event_id", md.ID),
		)

		hErr := handler(ctx, e)
		if hErr == nil {
			return nil
		}

		fields := logrus.Fields{
			"event_source":            md.Source,
			"event_type":              md.Type,
			"event_data_content_type": md.DataContentType,
			"processing_time":         time.Since(start).String(),
			"total_time":              time.Since(md.Time).String(),
		}

		fromContext(ctx).WithFields(fields).Errorf("error while handling event: %s %+v", hErr.Error(), e)

		return hErr
	}
}
