package remoteedit

import (
	"bytes"
	"crypto/md5"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
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

func (r RemoteEdit) getHash(stream io.ReadSeeker) ([]byte, error) {
	stream.Seek(0, io.SeekStart)

	h := md5.New()
	if _, err := io.Copy(h, stream); err != nil {
		return nil, err
	}

	stream.Seek(0, io.SeekStart)
	return h.Sum(nil), nil
}

func (r RemoteEdit) remoteEdit(baseName string, src io.ReadCloser, dst io.WriteCloser, shovel shovel.Shovel) error {
	tmpDirName, err := ioutil.TempDir("", "remote-edit-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDirName)

	tmpFileName := path.Join(tmpDirName, baseName)

	tmp, err := os.Create(tmpFileName)
	if err != nil {
		return err
	}
	defer tmp.Close()

	if err := shovel.CopyIn(tmp, src); err != nil {
		return err
	}

	// User editing the file
	startHash, err := r.getHash(tmp)
	if err != nil {
		return err
	}
	if err := r.Editor.Edit(tmpFileName); err != nil {
		return err
	}
	endHash, err := r.getHash(tmp)
	if err != nil {
		return err
	}

	// If nothing changed, don't write to final destination
	if bytes.Equal(startHash, endHash) {
		log.Printf("No change to input, not writing to the destination")
		return nil
	}

	// Write to final destination
	if err := shovel.CopyOut(dst, tmp); err != nil {
		return err
	}

	return nil
}
