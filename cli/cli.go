package cli

import (
	"net/url"
	"techiecaro/remblob/core"

	"github.com/willabides/kongplete"
)

type editCmd struct {
	SourcePath      *url.URL `arg:"" name:"source_path" help:"Location of the file to edit." predictor:"path"`
	DestinationPath *url.URL `arg:"" name:"destination_path" optional:"" help:"Final location of the edited file, if different." predictor:"path"`
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
	SourcePath *url.URL `arg:"" name:"source_path" help:"Location of the file to view." predictor:"path"`
}

func (v viewCmd) Run() error {
	return core.View(*v.SourcePath)
}

var Cli struct {
	Edit editCmd `cmd help:"Edits a remote blob and optionally stores it elsewhere."`
	View viewCmd `cmd help:"Views a remote blob."`

	// Competion
	// Use hidden empty command so the Kong does not complain on prompt
	Hidden             struct{}                     `cmd:"" help:"A hidden command" hidden:""`
	InstallCompletions kongplete.InstallCompletions `cmd:"" help:"install shell completions"`
}
