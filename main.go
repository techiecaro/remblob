package main

import (
	"net/url"

	"github.com/alecthomas/kong"
	"techiecaro.com/remote-edit/cmd"
)

var cli struct {
	SourcePath      *url.URL `arg:"" name:"source_path" placeholder:"aa" help:"Location of the file to edit."`
	DestinationPath *url.URL `arg:"" name:"destination_path" optional:"" help:"Final location of the edited file, if different."`
}

func main() {
	ctx := kong.Parse(
		&cli,
		kong.Name("remote-edit"),
		kong.Description(`
			Edit remote file locally.

			Example executions:
			remote-edit s3://a-bucket/path/blob.json
			remote-edit blob.json s3://a-bucket/path/blob.json.gz
		`),
		kong.UsageOnError(),
	)

	switch ctx.Command() {
	case "<source_path>":
		cmd.Main(*cli.SourcePath, *cli.SourcePath)
	case "<source_path> <destination_path>":
		cmd.Main(*cli.SourcePath, *cli.DestinationPath)
	default:
		panic(ctx.Command())
	}
}
