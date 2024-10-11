package idempotency

import (
	"context"
	"errors"
	"fmt"

	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

func SubscriberInterceptor(st Storage) eventbus.SubscriberInterceptor {
	return func(ctx context.Context, md *event.Metadata, e interface{}, handler eventbus.Handler) (err error) {
		if err = st.Lock(ctx, md.Type, md.ID); err != nil {
			if errors.Is(err, ErrEventReprocessing) {
				return eventbus.NewUnprocessableEventError(fmt.Errorf("%w event [%s] with id [%s]", md.Type, md.ID))
			}

			return fmt.Errorf("idempotency: marking event [%s] id [%s] for processing: %w", md.Type, md.ID, err)
		}

		defer func() {
			err = st.Unlock(ctx, md.Type, md.ID)
		}()

		if err = handler(ctx, e); err != nil {
			return err
		}

		if err = st.Done(ctx, md.Type, md.ID); err != nil {
			return fmt.Errorf("idempotency: marking event [%s] id [%s] as done: %w", md.Type, md.ID, err)
		}

		return nil
	}
}

var ErrEventReprocessing = fmt.Errorf("idempotency: event reprocessing is prohibited")

type Storage interface {
	Lock(ctx context.Context, eventType, id string) error
	Unlock(ctx context.Context, eventType, id string) error
	Done(ctx context.Context, eventType, id string) error
}
