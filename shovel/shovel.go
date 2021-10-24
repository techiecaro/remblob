package shovel

import (
	"io"
)

// A Shovel copies data between reader and writer.
type Shovel interface {
	CopyIn(dst io.WriteCloser, src io.ReadCloser) error
	CopyOut(dst io.WriteCloser, src io.ReadCloser) error
}
