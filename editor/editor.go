package editor

import (
	"os"
	"os/exec"
	"strings"
)

// An Editor applies modifications to local copy of the file.
type Editor interface {
	Edit(filename string) error
}

type EnvEditor struct{}

func (e EnvEditor) getEditor() []string {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vim"
	}

	return strings.Fields(editor)
}

func (e EnvEditor) Edit(filename string) error {
	editor := e.getEditor()

	editCmd := append(editor, filename)

	cmd := exec.Command(editCmd[0], editCmd[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout

	return cmd.Run()
}
