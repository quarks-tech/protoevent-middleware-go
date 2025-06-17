package logging

import (
	"context"
	"github.com/sirupsen/logrus"
)

type WithContext func(ctx context.Context, logger *logrus.Entry) context.Context

type FromContext func(context.Context) *logrus.Entry
