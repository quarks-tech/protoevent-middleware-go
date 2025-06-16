package logging

import (
	"context"

	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

type Logger interface {
	Errorf(string, ...any)
}

type Factory func(context.Context) Logger

func LoggingInterceptor(factory Factory) eventbus.SubscriberInterceptor {
	return func(ctx context.Context, md *event.Metadata, e any, handler eventbus.Handler) error {
		hErr := handler(ctx, e)
		if hErr != nil {
			factory(ctx).Errorf("error while handling event %s, %s %+v", md.Type, hErr.Error(), e)
		}

		return hErr
	}
}
