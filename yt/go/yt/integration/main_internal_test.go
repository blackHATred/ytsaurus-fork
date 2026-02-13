//go:build internal

package integration

import (
	"os"
	"testing"

	"github.com/blackHATred/ytsaurus-fork/yt/go/mapreduce"
)

func TestMain(m *testing.M) {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	os.Exit(m.Run())
}
