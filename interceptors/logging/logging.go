package logging

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"

	"github.com/quarks-tech/protoevent-middleware-go/interceptors/eventstart"
)

func LoggingInterceptor(withContext WithContext, fromContext FromContext) eventbus.SubscriberInterceptor {
	return func(ctx context.Context, md *event.Metadata, e any, handler eventbus.Handler) error {
		ctx = withContext(
			eventstart.WithContext(
				withContext(
					ctx,
					fromContext(ctx),
				),
				eventstart.Now(),
			),
			logrus.WithField("request_id", md.ID),
		)

		hErr := handler(ctx, e)
		if hErr == nil {
			return nil
		}

		fields := logrus.Fields{
			"client_version": md.SpecVersion,
			"referer":        md.Source,
			"method":         md.Type,
			"path":           md.DataContentType,
			"response_time":  time.Since(eventstart.FromContext(ctx)).String(),
		} // fields are named as indexed fields in log storage

		fromContext(ctx).WithFields(fields).Errorf("error while handling event: %s %+v", hErr.Error(), e)

		return hErr
	}
}
