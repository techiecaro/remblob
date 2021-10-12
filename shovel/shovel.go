package shovel

import (
	"io"
	"path"
)

// A Shovel copies data between reader and writer.
type Shovel interface {
	CopyIn(dst io.WriteCloser, src io.ReadCloser) error
	CopyOut(dst io.WriteCloser, src io.ReadCloser) error
}

// IsCompressed checks should the filename go though the compression/decompression
func IsCompressed(filename string) bool {
	return path.Ext(filename) == ".gz"
}
