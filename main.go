package main

import (
	"os"
	"techiecaro/remblob/cli"

	"github.com/alecthomas/kong"
)

const appName = "remblob"
const appDescription = `
	Edit remote files locally.

	Example executions:
	remblob edit s3://a-bucket/path/blob.json
	remblob edit blob.json s3://a-bucket/path/blob.json.gz
	remblob view s3://a-bucket/path/blob.json
`

func main() {
	parser := kong.Must(
		&cli.Cli,
		kong.Name(appName),
		kong.Description(appDescription),
		kong.UsageOnError(),
	)

	cli.AddCompletion(parser)

	ctx, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)

	err = ctx.Run()
	parser.FatalIfErrorf(err)
}
