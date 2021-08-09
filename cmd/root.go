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

	f, err := os.Open(args[0])
	if err != nil {
		log.Fatal(err)
	}

	remoteEdit(f)
}

func runEditor(fn string) {
	cmd := exec.Command("vim", fn)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout

	if err := cmd.Run(); err != nil {
		log.Fatal(err)
	}
}

func remoteEdit(src io.Reader) {
	tmpDirName, err := ioutil.TempDir("", "remote-edit-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDirName)

	tmpFileName := path.Join(tmpDirName, "tmp-file")

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
