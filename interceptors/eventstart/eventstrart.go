package eventstart

import (
	"context"
	"time"
)

var Now = time.Now

type key struct{}

func WithContext(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, key{}, t)
}

func FromContext(ctx context.Context) time.Time {
	if l := ctx.Value(key{}); l != nil {
		return l.(time.Time)
	}

	return Now()
}
