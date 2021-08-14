package storage

import (
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
		file, err := os.OpenFile(l.uri, os.O_RDONLY, 0755)
		if err != nil {
			return 0, err
		}
		l.localFile = file
	}

	return l.localFile.Read(p)
}

func (l *localFileStorage) Write(p []byte) (n int, err error) {
	if l.localFile == nil {
		file, err := os.OpenFile(l.uri, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return 0, err
		}
		l.localFile = file
	}
	return l.localFile.Write(p)
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
