package storage_test

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"techiecaro/remblob/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

var files = []string{
	"1.txt",
	"2.txt",
	".txt",
	"a/a1.txt",
	"a/a2.txt",
	"a/b/b1.txt",
	"a/b/b2.txt",
	"a/b/c/c1.txt",
	"a/b/c/c2.txt",
	"a/b/d/e/e1.txt",
	"x",
	"z",
}

func uriToPath(uris []url.URL) []string {
	paths := make([]string, len(uris))
	for i, uri := range uris {
		paths[i] = uri.String()
	}

	return paths
}

func createTestFileStructure(t *testing.T) string {
	dir := t.TempDir()

	for _, name := range files {
		fullPath := path.Join(dir, name)
		os.MkdirAll(path.Dir(fullPath), 0700)
		if _, err := os.Create(fullPath); err != nil {
			t.Fatal(err)
		}
	}

	return dir
}

func mustStrToURI(t *testing.T, path string) url.URL {
	uri, err := url.Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	return *uri
}

func mustGetCWD(t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func mustChdir(t *testing.T, dir string) {
	err := os.Chdir(dir)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLocalStorageSuggestions(t *testing.T) {
	topLevel := []string{"./.txt", "./1.txt", "./2.txt", "./a", "./x", "./z"}
	topLevelNoDot := []string{".txt", "1.txt", "2.txt", "a", "x", "z"}

	cases := []struct {
		prefix   string
		expected []string
		cwd      string
	}{
		{
			prefix:   ".",
			expected: topLevel,
			cwd:      "",
		},
		{
			prefix:   "",
			expected: topLevelNoDot,
			cwd:      "",
		},
		{
			prefix:   "./b",
			expected: topLevel,
			cwd:      "",
		},
		{
			prefix:   "b",
			expected: topLevel,
			cwd:      "",
		},
		{
			prefix:   "1.txt",
			expected: topLevel,
			cwd:      "",
		},
		{
			prefix:   "file://",
			expected: []string{"file://.txt", "file://1.txt", "file://2.txt", "file://a", "file://x", "file://z"},
			cwd:      "",
		},
		{
			prefix:   "file://a",
			expected: []string{"file://a/a1.txt", "file://a/a2.txt", "file://a/b"},
			cwd:      "",
		},
		{
			prefix:   "file://..",
			expected: []string{"file://../b1.txt", "file://../b2.txt", "file://../c", "file://../d"},
			cwd:      "a/b/c",
		},
		{
			prefix:   "./a",
			expected: []string{"./a/a1.txt", "./a/a2.txt", "./a/b"},
			cwd:      "",
		},
		{
			prefix:   "a",
			expected: []string{"a/a1.txt", "a/a2.txt", "a/b"},
			cwd:      "",
		},
		{
			prefix:   "a/b",
			expected: []string{"a/b/b1.txt", "a/b/b2.txt", "a/b/c", "a/b/d"},
			cwd:      "",
		},
		{
			prefix:   "a/b/c",
			expected: []string{"a/b/c/c1.txt", "a/b/c/c2.txt"},
			cwd:      "",
		},
		{
			prefix:   "a/b/d",
			expected: []string{"a/b/d/e"},
			cwd:      "",
		},
		{
			prefix:   "a/b/d/e",
			expected: []string{"a/b/d/e/e1.txt"},
			cwd:      "",
		},
		{
			prefix:   "a/x",
			expected: []string{"a/a1.txt", "a/a2.txt", "a/b"},
			cwd:      "",
		},
		{
			prefix:   "a/x/y",
			expected: []string{},
			cwd:      "",
		},
		{
			prefix:   ".",
			expected: []string{"./a1.txt", "./a2.txt", "./b"},
			cwd:      "a/",
		},
		{
			prefix:   "b",
			expected: []string{"b/b1.txt", "b/b2.txt", "b/c", "b/d"},
			cwd:      "a/",
		},
		{
			prefix:   ".",
			expected: []string{"./b1.txt", "./b2.txt", "./c", "./d"},
			cwd:      "a/b/",
		},
		{
			prefix:   "c",
			expected: []string{"c/c1.txt", "c/c2.txt"},
			cwd:      "a/b/",
		},
		{
			prefix:   "..",
			expected: []string{"../a1.txt", "../a2.txt", "../b"},
			cwd:      "a/b/",
		},
		{
			prefix:   "../..",
			expected: []string{"../../.txt", "../../1.txt", "../../2.txt", "../../a", "../../x", "../../z"},
			cwd:      "a/b/",
		},
		{
			prefix:   "../d",
			expected: []string{"../d/e"},
			cwd:      "a/b/c",
		},
		{
			prefix:   "../d/e",
			expected: []string{"../d/e/e1.txt"},
			cwd:      "a/b/c",
		},
	}

	for _, tc := range cases {
		testName := fmt.Sprintf("[%s][%s]", tc.cwd, tc.prefix)
		t.Run(testName, func(t *testing.T) {
			dir := createTestFileStructure(t)

			cwd := mustGetCWD(t)
			os.Chdir(dir)
			defer os.Chdir(cwd)

			if tc.cwd != "" {
				mustChdir(t, tc.cwd)
			}

			uriPrefix := mustStrToURI(t, tc.prefix)
			lister := storage.GetFileLister(uriPrefix)
			suggestion := lister(uriPrefix)

			assert.Equal(t, tc.expected, uriToPath(suggestion), "Invalid prompt")
		})
	}
}
