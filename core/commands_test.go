package core_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"techiecaro/remblob/core"
	"testing"

	"github.com/stretchr/testify/assert"
)

func readFile(t *testing.T, filename string) string {
	body, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func readFileGzip(t *testing.T, filename string) string {
	reader, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}

	gzreader, err := gzip.NewReader(reader)
	if err != nil {
		t.Fatal(err)
	}
	defer gzreader.Close()

	body, err := io.ReadAll(gzreader)
	if err != nil {
		t.Fatal(err, body)
	}
	return string(body)
}

func writeFile(t *testing.T, filename string, data string) {
	err := os.WriteFile(filename, []byte(data), 0700)
	if err != nil {
		t.Fatal(err)
	}
}

func writeFileGzip(t *testing.T, filename string, data string) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(data))
	w.Close()

	err := os.WriteFile(filename, b.Bytes(), 0700)
	if err != nil {
		t.Fatal(err)
	}
}

func appendFile(t *testing.T, filename string, data string) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if _, err = f.WriteString(data); err != nil {
		t.Fatal(err)
	}
}

func testFileURL(t *testing.T, directory string, name string) url.URL {
	fullPath := path.Join(directory, name)

	fileURL, err := url.Parse(fullPath)
	if err != nil {
		t.Fatal(err)
	}

	return *fileURL
}

func createTestFile(t *testing.T, directory string, name string, body string) url.URL {
	fileURL := testFileURL(t, directory, name)

	writeFile(t, fileURL.String(), body)

	return fileURL
}

// FakeEditor will add some text to the body and exit
type FakeEditor struct {
	body       string
	appendWith string
	t          *testing.T
}

func (e *FakeEditor) Edit(filename string) error {
	e.body = readFile(e.t, filename)
	appendFile(e.t, filename, e.appendWith)
	return nil
}

func TestViewCommand(t *testing.T) {
	inputBody := "test"
	changes := []struct {
		name   string
		change string
	}{
		{
			name:   "no-change",
			change: "",
		},
		{
			name:   "change",
			change: " - extra data",
		},
	}

	for _, tc := range changes {
		t.Run(tc.name, func(t *testing.T) {
			rootDir := t.TempDir()
			src := createTestFile(t, rootDir, "input.txt", inputBody)
			fakeEditor := &FakeEditor{t: t, appendWith: tc.change}

			err := core.View(src, fakeEditor)

			outputBody := readFile(t, src.String())

			assert.NoError(t, err)
			// View discards any changes being made
			assert.Equal(t, inputBody, outputBody)
			assert.Equal(t, inputBody, fakeEditor.body)
		})
	}
}

func TestEditCommandSameFile(t *testing.T) {
	inputBody := "test"
	inputFile := "input.txt"

	changes := []struct {
		name     string
		change   string
		expected string
	}{
		{
			name:     "no-change",
			change:   "",
			expected: "test",
		},
		{
			name:     "change",
			change:   " - extra data",
			expected: "test - extra data",
		},
	}

	for _, tc := range changes {
		t.Run(tc.name, func(t *testing.T) {
			// Input/Output file paths, only input exists
			rootDir := t.TempDir()
			src := createTestFile(t, rootDir, inputFile, inputBody)
			dst := testFileURL(t, rootDir, inputFile)

			// Edit
			fakeEditor := &FakeEditor{t: t, appendWith: tc.change}
			err := core.Edit(src, dst, fakeEditor)

			// Read result of edited file
			outputBody := readFile(t, dst.String())

			// Check for changes
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, outputBody)
			assert.Equal(t, inputBody, fakeEditor.body)
		})
	}
}

func TestEditCommandNoChangeDifferentFiles(t *testing.T) {
	inputBody := "test"
	change := ""
	inputFile := "input.txt"
	outputFile := "output.txt"

	// Input/Output file paths, only input exists
	rootDir := t.TempDir()
	src := createTestFile(t, rootDir, inputFile, inputBody)
	dst := testFileURL(t, rootDir, outputFile)

	// Edit
	fakeEditor := &FakeEditor{t: t, appendWith: change}
	err := core.Edit(src, dst, fakeEditor)

	// Read src file
	srcBody := readFile(t, src.String())

	// Check for changes - no change no new file
	assert.NoError(t, err)
	assert.NoFileExists(t, dst.String())
	assert.Equal(t, inputBody, fakeEditor.body)
	assert.Equal(t, inputBody, srcBody)
}

func TestEditCommandChangeDifferentFiles(t *testing.T) {
	inputBody := "test"
	change := " - change"
	expectedBody := "test - change"
	inputFile := "input.txt"
	outputFile := "output.txt"

	// Input/Output file paths, only input exists
	rootDir := t.TempDir()
	src := createTestFile(t, rootDir, inputFile, inputBody)
	dst := testFileURL(t, rootDir, outputFile)

	// Edit
	fakeEditor := &FakeEditor{t: t, appendWith: change}
	err := core.Edit(src, dst, fakeEditor)

	// Read src and dst files
	srcBody := readFile(t, src.String())
	dstBody := readFile(t, dst.String())

	// Check for changes
	assert.NoError(t, err)
	assert.Equal(t, expectedBody, dstBody)
	assert.Equal(t, inputBody, srcBody)
	assert.Equal(t, inputBody, fakeEditor.body)
}

func TestEditCommandChangeDifferentFilesGZip(t *testing.T) {
	inputBody := "test"
	change := " - change"
	expectedBody := "test - change"
	inputFile := "input.gz"
	outputFile := "output.gz"

	// Input/Output file paths, only input exists
	rootDir := t.TempDir()
	src := testFileURL(t, rootDir, inputFile)
	dst := testFileURL(t, rootDir, outputFile)
	fmt.Println(src)

	writeFileGzip(t, src.String(), inputBody)

	// Edit
	fakeEditor := &FakeEditor{t: t, appendWith: change}
	err := core.Edit(src, dst, fakeEditor)

	// Read src and dst files
	srcBody := readFileGzip(t, src.String())
	dstBody := readFileGzip(t, dst.String())

	// Check for changes
	assert.NoError(t, err)
	assert.Equal(t, expectedBody, dstBody)
	assert.Equal(t, inputBody, srcBody)
	assert.Equal(t, inputBody, fakeEditor.body)
}
