package storage

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

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
