package cli

import (
	"net/url"
	"techiecaro/remblob/core"
	"techiecaro/remblob/editor"

	"github.com/willabides/kongplete"
)

type editCmd struct {
	SourcePath      url.URL  `arg:"" name:"source_path" help:"Location of the file to edit." predictor:"path"`
	DestinationPath *url.URL `arg:"" name:"destination_path" optional:"" help:"Final location of the edited file, if different." predictor:"path"`
}

func (e editCmd) GetDestinationPath() url.URL {
	if e.DestinationPath != nil {
		return *e.DestinationPath
	}
	return e.SourcePath
}

func (e editCmd) Run() error {
	localEditor := editor.EnvEditor{}
	return core.Edit(e.SourcePath, e.GetDestinationPath(), localEditor)
}

type viewCmd struct {
	SourcePath url.URL `arg:"" name:"source_path" help:"Location of the file to view." predictor:"path"`
}

func (v viewCmd) Run() error {
	localEditor := editor.EnvEditor{}
	return core.View(v.SourcePath, localEditor)
}

var Cli struct {
	Edit editCmd `cmd help:"Edits a remote blob and optionally stores it elsewhere."`
	View viewCmd `cmd help:"Views a remote blob."`

	// Competion
	InstallCompletions kongplete.InstallCompletions `cmd:"" help:"install shell completions"`
}
