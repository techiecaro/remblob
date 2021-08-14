/*
Copyright © 2021 Karol Duleba <karolduleba@gmail.com>

*/
package cmd

import (
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

	remoteEdit(path.Base(fnIn), fIn, fOut)
}

func getEditor() []string {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vim"
	}

	return strings.Fields(editor)
}

func runEditor(fn string) {
	editor := getEditor()

	editCmd := append(editor, fn)

	cmd := exec.Command(editCmd[0], editCmd[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout

	if err := cmd.Run(); err != nil {
		log.Fatal(err)
	}
}

func remoteEdit(baseName string, src io.ReadCloser, dst io.WriteCloser) {
	tmpDirName, err := ioutil.TempDir("", "remote-edit-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDirName)

	tmpFileName := path.Join(tmpDirName, baseName)

	tmp, err := os.Create(tmpFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer tmp.Close()

	// Copy the src to temp destination
	if n, err := io.Copy(tmp, src); err != nil {
		panic(err)
	} else {
		log.Printf("Copied %v bytes to temp\n", n)
	}
	// Copy is made, close the source
	src.Close()

	// User editing the file
	runEditor(tmpFileName)

	// Move to the begging of the file
	tmp.Seek(0, io.SeekStart)

	// Copy the temp destination to dst
	if n, err := io.Copy(dst, tmp); err != nil {
		panic(err)
	} else {
		log.Printf("Copied %v bytes to final destination\n", n)
	}
	// Copy is made, close the source
	dst.Close()

	fmt.Println("Temp dir name:", tmpFileName)
}
