package migrate

import (
	"context"

	"github.com/blackHATred/ytsaurus-fork/yt/go/schema"
	"github.com/blackHATred/ytsaurus-fork/yt/go/ypath"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt"
)

// Create creates new dynamic table with provided schema.
func Create(ctx context.Context, yc yt.Client, path ypath.Path, schema schema.Schema) error {
	_, err := yc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Recursive: true,
		Attributes: map[string]any{
			"dynamic": true,
			"schema":  schema,
		},
	})

	return err
}
