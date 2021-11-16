package cli_test

import (
	"os"
	"path"
	"techiecaro/remblob/cli"
	"testing"

	"github.com/posener/complete"
	"github.com/stretchr/testify/assert"
)

func createTestFileStructure(t *testing.T) string {
	dir := t.TempDir()

	var files = []string{"1.txt", "2.txt", "a/a1.txt"}

	for _, name := range files {
		fullPath := path.Join(dir, name)
		os.MkdirAll(path.Dir(fullPath), 0700)
		if _, err := os.Create(fullPath); err != nil {
			t.Fatal(err)
		}
	}

	return dir
}

func mustGetCWD(t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestPathPredictor(t *testing.T) {
	cases := []struct {
		prefix   string
		expected []string
	}{
		{
			prefix:   "",
			expected: []string{"./", "file://", "s3://"},
		},
		{
			prefix:   ".",
			expected: []string{"./", "file://", "s3://", "./1.txt", "./2.txt", "./a"},
		},
		{
			prefix:   "a/",
			expected: []string{"./", "file://", "s3://", "a/a1.txt"},
		},
		{
			prefix:   "./a/",
			expected: []string{"./", "file://", "s3://", "./a/a1.txt"},
		},
		{
			prefix:   "file://",
			expected: []string{"./", "file://", "s3://", "file://1.txt", "file://2.txt", "file://a"},
		},
		{
			prefix:   "file://a",
			expected: []string{"./", "file://", "s3://", "file://a/a1.txt"},
		},
		{
			prefix:   "s3://",
			expected: []string{"./", "file://", "s3://"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.prefix, func(t *testing.T) {
			dir := createTestFileStructure(t)

			cwd := mustGetCWD(t)
			os.Chdir(dir)
			defer os.Chdir(cwd)

			args := complete.Args{
				Last:          tc.prefix,
				All:           []string{"not-in-use"},
				Completed:     []string{"not-in-use"},
				LastCompleted: "not-in-use",
			}

			predictor := cli.NewPathPredictor()

			suggestions := predictor.Predict(args)

			assert.Equal(t, tc.expected, suggestions, "Invalid prompt")
		})
	}
}
