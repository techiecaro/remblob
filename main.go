package main

import (
	"net/url"

	"techiecaro/remblob/core"

	"github.com/alecthomas/kong"
)

var cli struct {
	Edit struct {
		SourcePath      *url.URL `arg:"" name:"source_path" placeholder:"aa" help:"Location of the file to edit."`
		DestinationPath *url.URL `arg:"" name:"destination_path" optional:"" help:"Final location of the edited file, if different."`
	} `cmd`
	View struct {
		SourcePath *url.URL `arg:"" name:"source_path" placeholder:"aa" help:"Location of the file to view."`
	} `cmd`
}

func main() {
	ctx := kong.Parse(
		&cli,
		kong.Name("remblob"),
		kong.Description(`
			Edit remote file locally.

			Example executions:
			remblob edit s3://a-bucket/path/blob.json
			remblob edit blob.json s3://a-bucket/path/blob.json.gz
			remblob view s3://a-bucket/path/blob.json
		`),
		kong.UsageOnError(),
	)

	switch ctx.Command() {
	case "edit <source_path>":
		core.Edit(*cli.Edit.SourcePath, *cli.Edit.SourcePath)
	case "edit <source_path> <destination_path>":
		core.Edit(*cli.Edit.SourcePath, *cli.Edit.DestinationPath)
	case "view <source_path>":
		core.View(*cli.View.SourcePath)
	default:
		panic(ctx.Command())
	}
}
