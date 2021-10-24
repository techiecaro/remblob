package remoteedit

import (
	"io/ioutil"
	"os"
	"path"
)

type namedTempFile struct {
	file       *os.File
	tmpDirName string
}

func newNamedTempFile(baseName string) (*namedTempFile, error) {
	tmpDirName, err := ioutil.TempDir("", "remote-edit-*")
	if err != nil {
		return nil, err
	}

	tmpFileName := path.Join(tmpDirName, baseName)

	tmp, err := os.Create(tmpFileName)
	if err != nil {
		os.RemoveAll(tmpDirName)
		return nil, err
	}

	tempFile := &namedTempFile{
		file:       tmp,
		tmpDirName: tmpDirName,
	}

	return tempFile, nil
}

func (n namedTempFile) Close() error {
	if err := os.RemoveAll(n.tmpDirName); err != nil {
		return err
	}

	return n.file.Close()
}
