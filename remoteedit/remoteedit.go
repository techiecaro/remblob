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

func Edit(source url.URL, destination url.URL) {
	src := storage.GetFileStorage(source)
	dst := storage.GetFileStorage(destination)

	shovel := shovel.MultiShovel{
		SourceCompressed:      shovel.IsCompressed(source.String()),
		DestinationCompressed: shovel.IsCompressed(destination.String()),
	}

	baseName := path.Base(source.String())
	localEditor := editor.EnvEditor{}

	if err := remoteEdit(baseName, src, dst, shovel, localEditor); err != nil {
		log.Fatal(err)
	}
}

func remoteEdit(baseName string, src io.ReadCloser, dst io.WriteCloser, shovel shovel.Shovel, localEditor editor.Editor) error {
	// Create file with a nice name, inside temp folder. Close to remove it
	tmp, err := newNamedTempFile(baseName)
	if err != nil {
		return err
	}
	defer tmp.Close()

	// Copy to local file, ready for the editor
	if err := shovel.CopyIn(tmp.file, src); err != nil {
		return err
	}

	// User editing the file
	changes, err := localEdit(tmp.file, localEditor)
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
