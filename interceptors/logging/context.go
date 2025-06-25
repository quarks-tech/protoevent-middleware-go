package logging

import "context"

const contextKey = "logger-cmcbmj0tl0000cg02ifqzvqeo" // same key as microkit/pkg/logger to avoid collisions

func WithContext(ctx context.Context, entry Logger) context.Context {
	return context.WithValue(ctx, contextKey, entry)
}

func FromContext(ctx context.Context) Logger {
	return ctx.Value(contextKey).(Logger)
}
