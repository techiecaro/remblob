package shovel

import "io"

// A PlainShovel copies uncompressed
type PlainShovel struct{}

// CopyIn copies data from reader to writer. Then it closes the reader.
func (p PlainShovel) CopyIn(dst io.WriteCloser, src io.ReadCloser) error {
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return src.Close()
}

// CopyOut copies data from reader to writer. Then it closes the writer.
func (p PlainShovel) CopyOut(dst io.WriteCloser, src io.ReadCloser) error {
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return dst.Close()
}
