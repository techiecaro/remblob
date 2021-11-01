package main

import (
	"net/url"
	"os"
	"techiecaro/remblob/core"

	"github.com/alecthomas/kong"
	"github.com/willabides/kongplete"
)

const appName = "remblob"
const appDescription = `
	Edit remote files locally.

	Example executions:
	remblob edit s3://a-bucket/path/blob.json
	remblob edit blob.json s3://a-bucket/path/blob.json.gz
	remblob view s3://a-bucket/path/blob.json
`

type editCmd struct {
	SourcePath      *url.URL `arg:"" name:"source_path" help:"Location of the file to edit."`
	DestinationPath *url.URL `arg:"" name:"destination_path" optional:"" help:"Final location of the edited file, if different."`
}

func (e editCmd) GetDestinationPath() *url.URL {
	if e.DestinationPath != nil {
		return e.DestinationPath
	}
	return e.SourcePath
}

func (e editCmd) Run() error {
	return core.Edit(*e.SourcePath, *e.GetDestinationPath())
}

type viewCmd struct {
	SourcePath *url.URL `arg:"" name:"source_path" help:"Location of the file to view."`
}

func (v viewCmd) Run() error {
	return core.View(*v.SourcePath)
}

var cli struct {
	Edit               editCmd                      `cmd help:"Edits a remote blob and optionally stores it elsewhere."`
	View               viewCmd                      `cmd help:"Views a remote blob."`
	InstallCompletions kongplete.InstallCompletions `cmd:"" help:"install shell completions"`
}

func main() {
	parser := kong.Must(
		&cli,
		kong.Name(appName),
		kong.Description(appDescription),
		kong.UsageOnError(),
	)

	ctx, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)

	err = ctx.Run()
	parser.FatalIfErrorf(err)
}
