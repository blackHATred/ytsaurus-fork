package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/blackHATred/ytsaurus-fork/yt/go/yt"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt/clienttest"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yttest"
)

func TestClientGoroutineLeakage(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	httpClient := clienttest.NewHTTPClient(t, ytEnv.L)
	defer httpClient.Stop()
	rpcClient := clienttest.NewRPCClient(t, ytEnv.L)
	defer rpcClient.Stop()

	_, err := httpClient.CreateNode(ctx, ytEnv.TmpPath(), yt.NodeTable, nil)
	require.NoError(t, err)

	_, err = rpcClient.CreateNode(ctx, ytEnv.TmpPath(), yt.NodeTable, nil)
	require.NoError(t, err)
}
