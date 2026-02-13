package multicell

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/blackHATred/ytsaurus-fork/yt/go/dockertest"
	"github.com/blackHATred/ytsaurus-fork/yt/go/mapreduce"
)

func TestMain(m *testing.M) {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := dockertest.InitYTsaurusContainer(
		ctx,
		dockertest.WithDynamicTables(),
		dockertest.WithRPCProxies(1),
		dockertest.WithDiscoveryServers(1),
		dockertest.WithSecondaryMasterCells(2),
	)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}
	defer func() {
		if err := c.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %v", err)
		}
	}()

	return m.Run()
}
