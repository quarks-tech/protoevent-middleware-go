package idempotency

import (
	"context"
	"errors"
	"fmt"

	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

func SubscriberInterceptor(st Store) eventbus.SubscriberInterceptor {
	return func(ctx context.Context, md *event.Metadata, e interface{}, handler eventbus.Handler) error {
		if err := st.MarkProcessing(ctx, md.ID); err != nil {
			if errors.Is(err, ErrNotUnique) {
				return eventbus.NewUnprocessableEventError(fmt.Errorf("event [%s] with id [%s] is not unique", md.Type, md.ID))
			}

			return fmt.Errorf("marking event [%s] id [%s] for processing: %w", md.Type, md.ID, err)
		}

		if err := handler(ctx, e); err != nil {
			return err
		}

		if err := st.MarkDone(ctx, md.ID); err != nil {
			return fmt.Errorf("marking event [%s] id [%s] as done: %w", md.Type, md.ID, err)
		}

		return nil
	}
}

var ErrNotUnique = fmt.Errorf("event not unique")

type Store interface {
	MarkProcessing(ctx context.Context, id string) error
	MarkDone(ctx context.Context, id string) error
}
