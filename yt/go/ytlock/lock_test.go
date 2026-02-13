package ytlock

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackHATred/ytsaurus-fork/yt/go/ypath"
)

func TestLockWithOptions(t *testing.T) {
	initialOpt := Options{CreateIfMissing: true}
	lock := NewLockOptions(nil, ypath.Path(""), initialOpt)
	require.Equal(t, lock.Options, initialOpt, "Lock should have given options")
}
