package storage

import (
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type localFileStorage struct {
	uri       string
	localFile *os.File
}

func getLocalFileStorage(uri url.URL) *localFileStorage {
	fs := new(localFileStorage)
	fs.uri = uriToPath(uri)
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
		l.localFile.Truncate(0)
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

func uriToPath(uri url.URL) string {
	strURI := uri.Path
	if uri.Host != "" {
		strURI = path.Join(uri.Host, uri.Path)
	}

	return strURI
}

func isDir(prefix string) bool {
	stat, err := os.Stat(prefix)
	if err != nil {
		return false
	}
	return stat.IsDir()
}

func localFileStorageLister(prefix url.URL) []url.URL {
	suggestions := []url.URL{}

	basePath := uriToPath(prefix)
	parentDir := basePath
	if !isDir(parentDir) {
		parentDir = path.Dir(parentDir)
	}

	files, err := ioutil.ReadDir(parentDir)
	if err != nil {
		return suggestions
	}

	separator := string(filepath.Separator)
	for _, file := range files {
		full := strings.Join([]string{strings.TrimSuffix(parentDir, separator), file.Name()}, separator)
		if prefix.Scheme != "" || basePath == "" {
			full = path.Join(full)
		}
		if uri, err := url.Parse(full); err == nil {
			uri.Scheme = prefix.Scheme
			suggestions = append(suggestions, *uri)
		}
	}

	return suggestions
}

func init() {
	registerFileStorage(
		registrationInfo{
			storage:           func(uri url.URL) FileStorage { return getLocalFileStorage(uri) },
			lister:            localFileStorageLister,
			prefixes:          []string{"", "file://"},
			completionPrompts: []string{"./"},
		},
	)
}
