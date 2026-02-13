package ytrpc_test

import (
	"context"
	"fmt"
	"time"

	"github.com/blackHATred/ytsaurus-fork/yt/go/ypath"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt/ytrpc"
	"github.com/blackHATred/ytsaurus-fork/yt/go/ytlog"
)

func ExampleNewCypressClient() {
	yc, err := ytrpc.NewCypressClient(&yt.Config{
		Proxy:             "hume",
		ReadTokenFromFile: true,
		Logger:            ytlog.Must(),
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ok, err := yc.NodeExists(ctx, ypath.Path("//home"), nil)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Node exists? %v\n", ok)
}
