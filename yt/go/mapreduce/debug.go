package mapreduce

import (
	"context"

	"github.com/blackHATred/ytsaurus-fork/yt/go/guid"
	"github.com/blackHATred/ytsaurus-fork/yt/go/schema"
	"github.com/blackHATred/ytsaurus-fork/yt/go/ypath"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt"
)

// StderrTableRow is a single row of operation stderr table.
type StderrTableRow struct {
	JobID     guid.GUID `yson:"job_id,key"`
	PartIndex int       `yson:"part_index,key"`
	Data      []byte    `yson:"data"`
}

// CoreTableRow is a single row of operation core table.
type CoreTableRow struct {
	JobID     guid.GUID `yson:"job_id,key"`
	CoreID    int       `yson:"core_id,key"`
	PartIndex int       `yson:"part_index,key"`
	Data      []byte    `yson:"data"`
}

var (
	stderrTableSchema = schema.MustInfer(&StderrTableRow{})
	coreTableSchema   = schema.MustInfer(&CoreTableRow{})
)

func CreateStderrTable(ctx context.Context, yc yt.Client, path ypath.Path, opts ...yt.CreateTableOption) (yt.NodeID, error) {
	opts = append(opts, yt.WithSchema(stderrTableSchema))
	return yt.CreateTable(ctx, yc, path, opts...)
}

func CreateCoreTable(ctx context.Context, yc yt.Client, path ypath.Path, opts ...yt.CreateTableOption) (yt.NodeID, error) {
	opts = append(opts, yt.WithSchema(coreTableSchema))
	return yt.CreateTable(ctx, yc, path, opts...)
}
