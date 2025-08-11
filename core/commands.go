package core

import (
	"fmt"
	"io"
	"net/url"

	"techiecaro/remblob/editor"
	"techiecaro/remblob/shovel"
	"techiecaro/remblob/storage"
)

func Edit(source url.URL, destination url.URL, localEditor editor.Editor) error {
	src, err := storage.GetFileStorage(source)
	if err != nil {
		return err
	}

	var dst storage.FileStorage
	// Reuse same storage instance when editing the same file to preserve metadata
	if source.String() == destination.String() {
		dst = src
	} else {
		dst, err = storage.GetFileStorage(destination)
		if err != nil {
			return err
		}

		// Metadata transfer will be handled in remoteEditWithMetadata after reading
	}

	shovelInstance := &shovel.MultiShovel{
		SourceCompressed:      isCompressed(source),
		DestinationCompressed: isCompressed(destination),
		SourceParquet:         isParquet(source),
		DestinationParquet:    isParquet(destination),
	}
	baseName := getBaseName(source)

	return remoteEditWithMetadata(baseName, src, dst, shovelInstance, localEditor)
}

func View(source url.URL, localEditor editor.Editor) error {
	src, err := storage.GetFileStorage(source)
	if err != nil {
		return err
	}

	// For view mode, only care about source format
	shovelInstance := &shovel.MultiShovel{
		SourceCompressed:      isCompressed(source),
		DestinationCompressed: false, // Not used in view mode
		SourceParquet:         isParquet(source),
		DestinationParquet:    false, // Not used in view mode
	}
	baseName := getBaseName(source)

	return remoteView(baseName, src, shovelInstance, localEditor)
}

// remoteEditWithMetadata handles editing with metadata preservation
func remoteEditWithMetadata(baseName string, src storage.FileStorage, dst storage.FileStorage, shovel shovel.Shovel, localEditor editor.Editor) error {
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

	// Transfer metadata if both source and destination support it (and they're different instances)
	if src != dst {
		if srcMeta, ok := src.(storage.MetadataCapable); ok {
			if dstMeta, ok := dst.(storage.MetadataCapable); ok {
				metadata := srcMeta.GetMetadata()
				if err := dstMeta.SetMetadata(metadata); err != nil {
					return fmt.Errorf("failed to transfer metadata: %w", err)
				}
			}
		}
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
