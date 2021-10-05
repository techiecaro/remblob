package cmd

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strings"

	"techiecaro/remote-edit/storage"
)

func Main(source url.URL, destination url.URL) {
	fIn := storage.GetFileStorage(source)
	fOut := storage.GetFileStorage(destination)

	if err := remoteEdit(path.Base(source.String()), fIn, fOut); err != nil {
		log.Fatal(err)
	}
}

func getEditor() []string {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vim"
	}

	return strings.Fields(editor)
}

func runEditor(fn string) error {
	editor := getEditor()

	editCmd := append(editor, fn)

	cmd := exec.Command(editCmd[0], editCmd[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout

	return cmd.Run()
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

func remoteEdit(baseName string, src io.ReadCloser, dst io.WriteCloser) error {
	tmpDirName, err := ioutil.TempDir("", "remote-edit-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDirName)

	tmpFileName := path.Join(tmpDirName, baseName)
	isCompressed := getIsCompressed(baseName)

	tmp, err := os.Create(tmpFileName)
	if err != nil {
		return err
	}
	defer tmp.Close()

	if err := copyFile(tmp, src, true, isCompressed); err != nil {
		return err
	}

	// User editing the file
	startHash, err := getHash(tmp)
	if err != nil {
		return err
	}
	if err := runEditor(tmpFileName); err != nil {
		return err
	}
	endHash, err := getHash(tmp)
	if err != nil {
		return err
	}

	// If nothing changed, don't write to final destination
	if bytes.Equal(startHash, endHash) {
		log.Printf("No change to input, not writing to the destination")
		return nil
	}

	tmp.Seek(0, io.SeekStart)

	// Write to final destination
	if err := copyFile(dst, tmp, false, isCompressed); err != nil {
		return err
	}

	return nil
}

func getIsCompressed(filename string) bool {
	return path.Ext(filename) == ".gz"
}

func copyFile(dst io.WriteCloser, src io.ReadCloser, input bool, compressed bool) error {
	// Copy the temp destination to dst
	finalSrc := src
	finalDst := dst

	if compressed {
		if input {
			decompressedReader, err := gzip.NewReader(src)
			if err != nil {
				return err
			}
			finalSrc = decompressedReader
		} else {
			compressionWriter := gzip.NewWriter(dst)
			finalDst = compressionWriter
		}
	}

	if _, err := io.Copy(finalDst, finalSrc); err != nil {
		return err
	}

	// Copy is made, close the source/destination
	var toClose []io.Closer
	if input {
		if src != finalSrc {
			toClose = append(toClose, finalSrc)
		}
		toClose = append(toClose, src)
	} else {
		if dst != finalDst {
			toClose = append(toClose, finalDst)
		}
		toClose = append(toClose, dst)
	}

	for _, closer := range toClose {
		if err := closer.Close(); err != nil {
			return err
		}
	}

	return nil
}
