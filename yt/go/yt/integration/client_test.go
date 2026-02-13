package integration

import (
	"context"
	"testing"

	"github.com/blackHATred/ytsaurus-fork/yt/go/yt"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt/clienttest"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yttest"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
)

type Suite struct {
	*yttest.Env
}

func NewSuite(t *testing.T) *Suite {
	return &Suite{Env: yttest.New(t)}
}

type ClientTest struct {
	Name     string
	Test     func(ctx context.Context, t *testing.T, yc yt.Client)
	SkipRPC  bool
	SkipHTTP bool
}

func (s *Suite) RunClientTests(t *testing.T, tests []ClientTest) {
	httpClient := clienttest.NewHTTPClient(t, s.L)
	rpcClient := clienttest.NewRPCClient(t, s.L)

	for _, tc := range []struct {
		name   string
		client yt.Client
	}{
		{name: "http", client: httpClient},
		{name: "rpc", client: rpcClient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, test := range tests {
				skip := (tc.name == "rpc" && test.SkipRPC) ||
					(tc.name == "http" && test.SkipHTTP)
				if skip {
					continue
				}

				t.Run(test.Name, func(t *testing.T) {
					ctx := ctxlog.WithFields(s.Ctx, log.String("subtest_name", t.Name()))
					test.Test(ctx, t, tc.client)
				})
			}
		})
	}
}
