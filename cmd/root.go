/*
Copyright © 2021 Karol Duleba <karolduleba@gmail.com>

*/
package cmd

import (
	"bytes"
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

	tmp, err := os.Create(tmpFileName)
	if err != nil {
		return err
	}
	defer tmp.Close()

	// Copy the src to temp destination
	if _, err := io.Copy(tmp, src); err != nil {
		return err
	}
	// Copy is made, close the source
	if err := src.Close(); err != nil {
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

	if bytes.Equal(startHash, endHash) {
		log.Printf("No change to input, not writing to the destination")
		return nil
	}

	tmp.Seek(0, io.SeekStart)

	// Copy the temp destination to dst
	if _, err := io.Copy(dst, tmp); err != nil {
		return err
	}
	// Copy is made, close the source
	if err := dst.Close(); err != nil {
		return err
	}

	return nil
}
