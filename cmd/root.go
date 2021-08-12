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

	fIn, err := os.Open(fnIn)
	if err != nil {
		log.Fatal(err)
	}

	remoteEdit(path.Base(fnIn), fIn)
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

func remoteEdit(baseName string, src io.Reader) {
	tmpDirName, err := ioutil.TempDir("", "remote-edit-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDirName)

	tmpFileName := path.Join(tmpDirName, baseName)

	w, err := os.Create(tmpFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	// Copy the src to temp destination
	n, err := io.Copy(w, src)
	if err != nil {
		panic(err)
	}
	log.Printf("Copied %v bytes\n", n)

	runEditor(tmpFileName)

	fmt.Println("Temp dir name:", tmpFileName)
}
