package remoteedit

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"path"

	"techiecaro/remote-edit/editor"
	"techiecaro/remote-edit/shovel"
	"techiecaro/remote-edit/storage"
)

type RemoteEdit struct {
	Editor editor.Editor
}

func NewRemoteEditor() RemoteEdit {
	return RemoteEdit{
		Editor: editor.EnvEditor{},
	}
}

func (r RemoteEdit) Edit(source url.URL, destination url.URL) {
	src := storage.GetFileStorage(source)
	dst := storage.GetFileStorage(destination)

	shovel := shovel.MultiShovel{
		SourceCompressed:      shovel.IsCompressed(source.String()),
		DestinationCompressed: shovel.IsCompressed(destination.String()),
	}

	if err := r.remoteEdit(path.Base(source.String()), src, dst, shovel); err != nil {
		log.Fatal(err)
	}
}

func (r RemoteEdit) remoteEdit(baseName string, src io.ReadCloser, dst io.WriteCloser, shovel shovel.Shovel) error {
	// Create file with nice name, inside temp folder. Close to remove it
	tmp, err := newNamedTempFile(baseName)
	if err != nil {
		return err
	}
	defer tmp.Close()

	if err := shovel.CopyIn(tmp.file, src); err != nil {
		return err
	}

	// User editing the file
	changes, err := localEdit(tmp.file, r.Editor)
	if err != nil {
		return err
	}
	// If nothing changed, don't write to final destination
	if changes == false {
		fmt.Println("No change to input, not writing to the destination")
		return nil
	}

	// Write to final destination
	if err := shovel.CopyOut(dst, tmp.file); err != nil {
		return err
	}

	return nil
}
