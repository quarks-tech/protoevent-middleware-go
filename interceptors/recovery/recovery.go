package recovery

import (
	"context"
	"fmt"
	"runtime"

	"github.com/quarks-tech/protoevent-go/pkg/event"
	"github.com/quarks-tech/protoevent-go/pkg/eventbus"
)

type HandlerFunc func(p any) (err error)

type HandlerFuncContext func(ctx context.Context, p any) (err error)

func SubscriberInterceptor(opts ...Option) eventbus.SubscriberInterceptor {
	o := evaluateOptions(opts)
	return func(ctx context.Context, md *event.Metadata, event any, handler eventbus.Handler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recoverFrom(ctx, r, o.handlerFunc)
			}
		}()

		return handler(ctx, event)
	}
}

func recoverFrom(ctx context.Context, p any, r HandlerFuncContext) error {
	if r != nil {
		return r(ctx, p)
	}
	stack := make([]byte, 64<<10)
	stack = stack[:runtime.Stack(stack, false)]
	return &PanicError{Panic: p, Stack: stack}
}

type PanicError struct {
	Panic any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic caught: %v\n\n%s", e.Panic, e.Stack)
}
