package core

import (
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"techiecaro/remblob/editor"
)

func localEdit(tmp *os.File, localEditor editor.Editor) (bool, error) {
	// User editing the file
	startHash, err := getHash(tmp)
	if err != nil {
		return false, err
	}
	if err := localEditor.Edit(tmp.Name()); err != nil {
		return false, err
	}
	endHash, err := getHash(tmp)
	if err != nil {
		return false, err
	}

	changes := bytes.Equal(startHash, endHash) == false

	return changes, nil
}

func getHash(stream io.ReadSeeker) ([]byte, error) {
	stream.Seek(0, io.SeekStart)

	h := md5.New()
	if _, err := io.Copy(h, stream); err != nil {
		return nil, err
	}

	stream.Seek(0, io.SeekStart)
	return h.Sum(nil), nil
}
