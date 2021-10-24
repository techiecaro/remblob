package core

import (
	"fmt"
	"io"
	"log"
	"net/url"

	"techiecaro/remblob/editor"
	"techiecaro/remblob/shovel"
	"techiecaro/remblob/storage"
)

func Edit(source url.URL, destination url.URL) {
	src := storage.GetFileStorage(source)
	dst := storage.GetFileStorage(destination)

	shovel := shovel.MultiShovel{
		SourceCompressed:      isCompressed(source),
		DestinationCompressed: isCompressed(destination),
	}

	baseName := getBaseName(source)
	localEditor := editor.EnvEditor{}

	if err := remoteEdit(baseName, src, dst, shovel, localEditor); err != nil {
		log.Fatal(err)
	}
}

func View(source url.URL) {
	src := storage.GetFileStorage(source)

	shovel := shovel.MultiShovel{
		SourceCompressed:      isCompressed(source),
		DestinationCompressed: false, // Not in use
	}

	baseName := getBaseName(source)
	localEditor := editor.EnvEditor{}

	if err := remoteView(baseName, src, shovel, localEditor); err != nil {
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

func remoteView(baseName string, src io.ReadCloser, shovel shovel.Shovel, localEditor editor.Editor) error {
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
	if changes {
		fmt.Println("Running in a view mode. Changes were discarded!")
		return nil
	}

	return nil
}
