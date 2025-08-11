package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
)

var blobs = map[string][]string{
	"bucekt-a": []string{
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
		"abc/z1.txt",
		"abd/z2.txt",
		"x",
		"z",
	},
	"bucekt-b": []string{
		"r/a1.txt",
		"q/a2.txt",
		"r.txt",
	},
	"bucekt-b1": []string{},
	"bucekt-b2": []string{},
}

type mockS3Lister struct {
	Buckets map[string][]string
}

func (m *mockS3Lister) ListBuckets(context.Context, *s3.ListBucketsInput, ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	output := s3.ListBucketsOutput{}
	bucketNames := []string{}

	for bucketName := range m.Buckets {
		bucketNames = append(bucketNames, bucketName)
	}

	sort.Strings(bucketNames)

	for i := range bucketNames {
		output.Buckets = append(output.Buckets, types.Bucket{Name: &bucketNames[i]})
	}

	return &output, nil
}

func (m *mockS3Lister) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	output := s3.ListObjectsV2Output{}

	bucket := *params.Bucket

	keys := []string{}
	prefixesMap := map[string]bool{}
	prefixes := []string{}

	delimRegex := regexp.MustCompile(*params.Delimiter)

	for _, key := range m.Buckets[bucket] {
		if !strings.HasPrefix(key, *params.Prefix) {
			continue
		}
		suffix := strings.TrimPrefix(key, *params.Prefix)
		if strings.Contains(suffix, *params.Delimiter) {
			keyPrefix := fmt.Sprintf("%s%s%s", *params.Prefix, delimRegex.Split(suffix, 2)[0], *params.Delimiter)
			prefixesMap[keyPrefix] = true
		} else {
			keys = append(keys, key)
		}

	}

	for keyPrefix := range prefixesMap {
		prefixes = append(prefixes, keyPrefix)
	}

	sort.Strings(keys)
	for i := range keys {
		output.Contents = append(output.Contents, types.Object{Key: &keys[i]})
	}

	sort.Strings(prefixes)
	for i := range prefixes {
		output.CommonPrefixes = append(output.CommonPrefixes, types.CommonPrefix{Prefix: &prefixes[i]})
	}

	return &output, nil
}

// readCloserWrapper wraps a strings.Reader to implement io.ReadCloser
type readCloserWrapper struct {
	*strings.Reader
}

func (r *readCloserWrapper) Close() error {
	return nil
}

func newReadCloser(s string) io.ReadCloser {
	return &readCloserWrapper{strings.NewReader(s)}
}

type mockS3Client struct {
	*mockS3Lister
	objects map[string]*s3.GetObjectOutput
	puts    map[string]*s3.PutObjectInput
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := fmt.Sprintf("%s/%s", *params.Bucket, *params.Key)
	if obj, exists := m.objects[key]; exists {
		return obj, nil
	}
	return nil, fmt.Errorf("object not found: %s", key)
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := fmt.Sprintf("%s/%s", *params.Bucket, *params.Key)
	m.puts[key] = params
	return &s3.PutObjectOutput{}, nil
}

func TestS3StorageRead(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		key         string
		content     string
		expectError bool
	}{
		{
			name:        "successful read",
			bucket:      "test-bucket",
			key:         "test-file.txt",
			content:     "Hello, World!",
			expectError: false,
		},
		{
			name:        "read JSON file",
			bucket:      "my-bucket",
			key:         "data/config.json",
			content:     `{"name": "test", "value": 123}`,
			expectError: false,
		},
		{
			name:        "read empty file",
			bucket:      "empty-bucket",
			key:         "empty.txt",
			content:     "",
			expectError: true, // Empty files return EOF on first read
		},
		{
			name:        "file not found",
			bucket:      "missing-bucket",
			key:         "nonexistent.txt",
			content:     "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockS3Client{
				mockS3Lister: &mockS3Lister{Buckets: blobs},
				objects:      make(map[string]*s3.GetObjectOutput),
				puts:         make(map[string]*s3.PutObjectInput),
			}

			if !tt.expectError {
				key := fmt.Sprintf("%s/%s", tt.bucket, tt.key)
				mockClient.objects[key] = &s3.GetObjectOutput{
					Body: newReadCloser(tt.content),
				}
			}

			uri, err := url.Parse(fmt.Sprintf("s3://%s/%s", tt.bucket, tt.key))
			assert.NoError(t, err)
			storage := getS3FileStorage(*uri, mockClient)

			// Test reading
			data := make([]byte, len(tt.content)+100) // Extra space to test boundaries
			n, err := storage.Read(data)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, len(tt.content), n)
			assert.Equal(t, tt.content, string(data[:n]))

			// Test reading remaining data (should be empty/EOF after first read)
			data2 := make([]byte, 10)
			n2, err2 := storage.Read(data2)
			// After reading all content, subsequent reads should return EOF
			assert.Equal(t, 0, n2)
			if len(tt.content) == 0 {
				// Empty files may return EOF immediately on first read
				assert.Error(t, err2)
			} else {
				// Non-empty files return EOF after content is exhausted
				assert.Error(t, err2)
			}
		})
	}
}

func TestS3StorageWrite(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		key    string
		writes []string // Multiple writes to test buffering
		final  string   // Expected final content
	}{
		{
			name:   "single write",
			bucket: "test-bucket",
			key:    "single.txt",
			writes: []string{"Hello, World!"},
			final:  "Hello, World!",
		},
		{
			name:   "multiple writes",
			bucket: "test-bucket",
			key:    "multi.txt",
			writes: []string{"Hello, ", "World", "!"},
			final:  "Hello, World!",
		},
		{
			name:   "empty write",
			bucket: "test-bucket",
			key:    "empty.txt",
			writes: []string{""},
			final:  "",
		},
		{
			name:   "JSON content",
			bucket: "data-bucket",
			key:    "config.json",
			writes: []string{`{"name": "test", `, `"value": 123}`},
			final:  `{"name": "test", "value": 123}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockS3Client{
				mockS3Lister: &mockS3Lister{Buckets: blobs},
				objects:      make(map[string]*s3.GetObjectOutput),
				puts:         make(map[string]*s3.PutObjectInput),
			}

			uri, err := url.Parse(fmt.Sprintf("s3://%s/%s", tt.bucket, tt.key))
			assert.NoError(t, err)
			storage := getS3FileStorage(*uri, mockClient)

			// Test multiple writes
			totalWritten := 0
			for _, writeData := range tt.writes {
				n, err := storage.Write([]byte(writeData))
				assert.NoError(t, err)
				assert.Equal(t, len(writeData), n)
				totalWritten += n
			}

			assert.Equal(t, len(tt.final), totalWritten)

			// Close should trigger upload
			err = storage.Close()
			assert.NoError(t, err)

			// Verify upload happened with correct S3 parameters
			key := fmt.Sprintf("%s/%s", tt.bucket, tt.key)
			putInput, exists := mockClient.puts[key]
			assert.True(t, exists, "Object should have been uploaded")
			assert.Equal(t, tt.bucket, *putInput.Bucket)
			assert.Equal(t, tt.key, *putInput.Key)

			// Verify uploaded content
			uploadedData := make([]byte, len(tt.final)+10) // Extra space for empty case
			n, err := putInput.Body.Read(uploadedData)
			if len(tt.final) == 0 {
				// Empty content - expect EOF or 0 bytes read
				assert.Equal(t, 0, n)
				assert.Equal(t, "", string(uploadedData[:n]))
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(tt.final), n)
				assert.Equal(t, tt.final, string(uploadedData[:n]))
			}
		})
	}
}

func TestS3StorageReadWrite(t *testing.T) {
	// Test read-modify-write cycle
	originalContent := `{"version": 1, "data": "original"}`
	modifiedContent := `{"version": 2, "data": "modified"}`

	mockClient := &mockS3Client{
		mockS3Lister: &mockS3Lister{Buckets: blobs},
		objects: map[string]*s3.GetObjectOutput{
			"bucket/file.json": {
				Body: newReadCloser(originalContent),
			},
		},
		puts: make(map[string]*s3.PutObjectInput),
	}

	uri, err := url.Parse("s3://bucket/file.json")
	assert.NoError(t, err)
	storage := getS3FileStorage(*uri, mockClient)

	// Read original content
	readData := make([]byte, len(originalContent)+100)
	n, err := storage.Read(readData)
	assert.NoError(t, err)
	assert.Equal(t, originalContent, string(readData[:n]))

	// Write modified content
	writeN, err := storage.Write([]byte(modifiedContent))
	assert.NoError(t, err)
	assert.Equal(t, len(modifiedContent), writeN)

	// Close to trigger upload
	err = storage.Close()
	assert.NoError(t, err)

	// Verify upload with correct parameters and content
	putInput, exists := mockClient.puts["bucket/file.json"]
	assert.True(t, exists, "Modified file should have been uploaded")
	assert.Equal(t, "bucket", *putInput.Bucket)
	assert.Equal(t, "file.json", *putInput.Key)

	uploadedData := make([]byte, len(modifiedContent))
	readN, err := putInput.Body.Read(uploadedData)
	assert.NoError(t, err)
	assert.Equal(t, len(modifiedContent), readN)
	assert.Equal(t, modifiedContent, string(uploadedData))
}

func TestS3StorageClose(t *testing.T) {
	t.Run("close after read only", func(t *testing.T) {
		mockClient := &mockS3Client{
			mockS3Lister: &mockS3Lister{Buckets: blobs},
			objects: map[string]*s3.GetObjectOutput{
				"bucket/file.txt": {
					Body: newReadCloser("test content"),
				},
			},
			puts: make(map[string]*s3.PutObjectInput),
		}

		uri, err := url.Parse("s3://bucket/file.txt")
		assert.NoError(t, err)
		storage := getS3FileStorage(*uri, mockClient)

		// Read only
		data := make([]byte, 100)
		_, err = storage.Read(data)
		assert.NoError(t, err)

		// Close should not trigger upload
		err = storage.Close()
		assert.NoError(t, err)

		// Verify no upload happened
		assert.Empty(t, mockClient.puts)
	})

	t.Run("close after write only", func(t *testing.T) {
		mockClient := &mockS3Client{
			mockS3Lister: &mockS3Lister{Buckets: blobs},
			objects:      make(map[string]*s3.GetObjectOutput),
			puts:         make(map[string]*s3.PutObjectInput),
		}

		uri, err := url.Parse("s3://bucket/file.txt")
		assert.NoError(t, err)
		storage := getS3FileStorage(*uri, mockClient)

		// Write only
		content := "new content"
		_, err = storage.Write([]byte(content))
		assert.NoError(t, err)

		// Close should trigger upload
		err = storage.Close()
		assert.NoError(t, err)

		// Verify upload happened with correct parameters
		assert.Len(t, mockClient.puts, 1)
		putInput, exists := mockClient.puts["bucket/file.txt"]
		assert.True(t, exists, "Expected file should have been uploaded")
		assert.Equal(t, "bucket", *putInput.Bucket)
		assert.Equal(t, "file.txt", *putInput.Key)

		// Verify uploaded content
		uploadedData := make([]byte, len(content))
		n, err := putInput.Body.Read(uploadedData)
		assert.NoError(t, err)
		assert.Equal(t, len(content), n)
		assert.Equal(t, content, string(uploadedData))
	})

	t.Run("multiple close calls", func(t *testing.T) {
		mockClient := &mockS3Client{
			mockS3Lister: &mockS3Lister{Buckets: blobs},
			objects:      make(map[string]*s3.GetObjectOutput),
			puts:         make(map[string]*s3.PutObjectInput),
		}

		uri, err := url.Parse("s3://bucket/file.txt")
		assert.NoError(t, err)
		storage := getS3FileStorage(*uri, mockClient)

		_, err = storage.Write([]byte("content"))
		assert.NoError(t, err)

		// First close
		err = storage.Close()
		assert.NoError(t, err)

		// Second close should be safe
		err = storage.Close()
		assert.NoError(t, err)

		// Should still have only one upload with correct content
		assert.Len(t, mockClient.puts, 1)
		putInput, exists := mockClient.puts["bucket/file.txt"]
		assert.True(t, exists, "Expected file should have been uploaded exactly once")
		assert.Equal(t, "bucket", *putInput.Bucket)
		assert.Equal(t, "file.txt", *putInput.Key)

		// Verify uploaded content
		uploadedData := make([]byte, 7) // "content"
		n, err := putInput.Body.Read(uploadedData)
		assert.NoError(t, err)
		assert.Equal(t, 7, n)
		assert.Equal(t, "content", string(uploadedData))
	})
}

func TestS3MetadataPreservation(t *testing.T) {
	// Setup mock client with an object that has metadata
	originalMetadata := map[string]string{
		"custom-header": "custom-value",
		"user-data":     "important-info",
	}

	mockClient := &mockS3Client{
		mockS3Lister: &mockS3Lister{Buckets: blobs},
		objects: map[string]*s3.GetObjectOutput{
			"test-bucket/test-file.json": {
				Body:            newReadCloser(`{"test": "data"}`),
				Metadata:        originalMetadata,
				ContentType:     aws.String("application/json"),
				CacheControl:    aws.String("max-age=3600"),
				ContentEncoding: aws.String("gzip"),
			},
		},
		puts: make(map[string]*s3.PutObjectInput),
	}

	// Create S3 storage instance with mock client
	uri, err := url.Parse("s3://test-bucket/test-file.json")
	assert.NoError(t, err)
	storage := getS3FileStorage(*uri, mockClient)

	// Simulate editing: read the file
	data := make([]byte, 1024)
	n, err := storage.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, `{"test": "data"}`, string(data[:n]))

	// Simulate editing: write modified content
	modifiedContent := `{"test": "modified data"}`
	_, err = storage.Write([]byte(modifiedContent))
	assert.NoError(t, err)

	// Close to trigger upload
	err = storage.Close()
	assert.NoError(t, err)

	// Verify the upload happened
	putInput, exists := mockClient.puts["test-bucket/test-file.json"]
	assert.True(t, exists, "File should have been uploaded")

	// Verify content was modified
	uploadedData := make([]byte, len(modifiedContent))
	_, err = putInput.Body.Read(uploadedData)
	assert.NoError(t, err)
	assert.Equal(t, modifiedContent, string(uploadedData))

	// CRITICAL TEST: Verify metadata was preserved
	// This test will currently FAIL because metadata is not preserved
	assert.Equal(t, originalMetadata, putInput.Metadata, "Metadata should be preserved during edit")
	assert.Equal(t, aws.String("application/json"), putInput.ContentType, "ContentType should be preserved")
	assert.Equal(t, aws.String("max-age=3600"), putInput.CacheControl, "CacheControl should be preserved")
	assert.Equal(t, aws.String("gzip"), putInput.ContentEncoding, "ContentEncoding should be preserved")
}

func TestS3StorageSuggestions(t *testing.T) {
	client := &mockS3Lister{Buckets: blobs}

	cases := []struct {
		prefix   string
		expected []string
	}{
		{
			prefix:   "s3://",
			expected: []string{"s3://bucekt-a/", "s3://bucekt-b/", "s3://bucekt-b1/", "s3://bucekt-b2/"},
		},
		{
			prefix:   "s3://aaa",
			expected: []string{"s3://bucekt-a/", "s3://bucekt-b/", "s3://bucekt-b1/", "s3://bucekt-b2/"},
		},
		{
			prefix:   "s3://bucekt-a",
			expected: []string{"s3://bucekt-a/", "s3://bucekt-b/", "s3://bucekt-b1/", "s3://bucekt-b2/"},
		},
		{
			prefix:   "s3://bucekt-a/",
			expected: []string{"s3://bucekt-a/a/", "s3://bucekt-a/abc/", "s3://bucekt-a/abd/", "s3://bucekt-a/.txt", "s3://bucekt-a/1.txt", "s3://bucekt-a/2.txt", "s3://bucekt-a/x", "s3://bucekt-a/z"},
		},
		{
			prefix:   "s3://bucekt-a/a",
			expected: []string{"s3://bucekt-a/a/", "s3://bucekt-a/abc/", "s3://bucekt-a/abd/"},
		},
		{
			prefix:   "s3://bucekt-a/a/",
			expected: []string{"s3://bucekt-a/a/b/", "s3://bucekt-a/a/a1.txt", "s3://bucekt-a/a/a2.txt"},
		},
		{
			prefix:   "s3://bucekt-a/a/a",
			expected: []string{"s3://bucekt-a/a/a1.txt", "s3://bucekt-a/a/a2.txt"},
		},
		{
			prefix:   "s3://bucekt-a/a/b",
			expected: []string{"s3://bucekt-a/a/b/"},
		},
		{
			prefix:   "s3://bucekt-a/a/b/",
			expected: []string{"s3://bucekt-a/a/b/c/", "s3://bucekt-a/a/b/d/", "s3://bucekt-a/a/b/b1.txt", "s3://bucekt-a/a/b/b2.txt"},
		},
		{
			prefix:   "s3://bucekt-a/a/b/c",
			expected: []string{"s3://bucekt-a/a/b/c/"},
		},
		{
			prefix:   "s3://bucekt-a/a/b/c/",
			expected: []string{"s3://bucekt-a/a/b/c/c1.txt", "s3://bucekt-a/a/b/c/c2.txt"},
		},
		{
			prefix:   "s3://bucekt-a/az",
			expected: []string{},
		},
		{
			prefix:   "s3://bucekt-a/z",
			expected: []string{"s3://bucekt-a/z"},
		},
	}

	for _, tc := range cases {
		testName := fmt.Sprintf("[%s]", tc.prefix)
		t.Run(testName, func(t *testing.T) {
			prefix := mustStrToURI(t, tc.prefix)
			actual := s3FileStorageLister(prefix, client)
			assert.Equal(t, tc.expected, urisToPaths(actual), "Invalid prompt")
		})
	}
}
