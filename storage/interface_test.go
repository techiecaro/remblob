package storage_test

import (
	"techiecaro/remblob/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFileListerPrefixes(t *testing.T) {
	prefixes := storage.GetFileListerPrefixes()

	expected := []string{"./", "file://", "s3://"}

	assert.Equal(t, expected, prefixes, "Invalid prefixes")
}
