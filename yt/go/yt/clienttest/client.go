package clienttest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackHATred/ytsaurus-fork/yt/go/yt"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt/ythttp"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt/ytrpc"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewHTTPClient(t *testing.T, l log.Structured) yt.Client {
	t.Helper()

	yc, err := ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: l})
	require.NoError(t, err)

	return yc
}

func NewRPCClient(t *testing.T, l log.Structured) yt.Client {
	t.Helper()

	yc, err := ytrpc.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: l})
	require.NoError(t, err)

	return yc
}
