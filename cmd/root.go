package cmd

/*
Copyright © 2021 Karol Duleba <karolduleba@gmail.com>

*/

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"

	"techiecaro.com/remote-edit/storage"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:     "remote-edit <remote-file>",
	Short:   "Edit remote file locally",
	Example: "remote-edit s3://a-bucket/path/blob.json",
	Args:    cobra.ExactValidArgs(1),
	Run:     main,
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func main(cmd *cobra.Command, args []string) {
	fmt.Println("main called")

	fnIn := args[0]

	fIn := storage.GetFileStorage(fnIn)
	fOut := storage.GetFileStorage(fnIn)

	if err := remoteEdit(path.Base(fnIn), fIn, fOut); err != nil {
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
