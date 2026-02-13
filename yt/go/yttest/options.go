package yttest

import (
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt"
	"go.ytsaurus.tech/library/go/core/log"
)

type Option interface {
	isOption()
}

type loggerOption struct{ l log.Structured }

func WithLogger(l log.Structured) Option {
	return loggerOption{l: l}
}

func (o loggerOption) isOption() {}

type configOption struct{ c yt.Config }

func WithConfig(c yt.Config) Option {
	return configOption{c: c}
}

func (c configOption) isOption() {}
