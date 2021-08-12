package storage

import (
	"io"
	"os"
)

type localFileStorage struct {
	uri       string
	localFile *os.File
}

func getLocalFileStorage(uri string) *localFileStorage {
	fs := new(localFileStorage)
	fs.uri = uri
	fs.localFile = nil
	return fs
}

func (l *localFileStorage) Read(p []byte) (n int, err error) {
	if l.localFile == nil {
		file, err := os.Open(l.uri)
		if err != nil {
			return 0, err
		}
		l.localFile = file
	}

	bytes, err := l.localFile.Read(p)
	if err == io.EOF {
		l.Close()
	}

	return bytes, err
}

func (l *localFileStorage) Close() error {
	if l.localFile == nil {
		return nil
	}

	err := l.localFile.Close()
	if err != nil {
		return err
	}

	l.localFile = nil
	return nil
}
