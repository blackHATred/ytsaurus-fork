package main

import (
	"context"

	"github.com/blackHATred/ytsaurus-fork/yt/go/schema"
	"github.com/blackHATred/ytsaurus-fork/yt/go/ypath"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt"
	"github.com/blackHATred/ytsaurus-fork/yt/go/yt/ythttp"
)

func main() {
	ctx := context.Background()

	yc, _ := ythttp.NewClient(&yt.Config{Proxy: "freud"})

	// Creating table with a schema.
	tablePath := ypath.Path("//tmp/table-schema-example")
	tableSchema := schema.MustInfer(struct {
		UUID  int64  `yson:"uuid"`
		Login string `yson:"login"`
	}{})

	_, _ = yt.CreateTable(ctx, yc, tablePath, yt.WithSchema(tableSchema))

	// Getting schema of an existing table.
	_ = yc.GetNode(ctx, tablePath.Attr("schema"), &tableSchema, nil)

	// Altering schema of an existing table.
	newSchema := schema.MustInfer(struct {
		UUID  int64   `yson:"uuid"`
		Login string  `yson:"login"`
		Name  *string `yson:"name"`
	}{})

	_ = yc.AlterTable(ctx, tablePath, &yt.AlterTableOptions{Schema: &newSchema})
}
