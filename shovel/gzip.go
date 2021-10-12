package shovel

import (
	"compress/gzip"
	"io"
)

// A GzipShovel copies between uncompressed and compressed
type GzipShovel struct{}

// CopyIn copies data from reader to writer while uncompressing it with Gzip. Then it closes the reader.
func (g GzipShovel) CopyIn(dst io.WriteCloser, src io.ReadCloser) error {
	decompressedReader, err := gzip.NewReader(src)
	if err != nil {
		return err
	}

	if _, err := io.Copy(dst, decompressedReader); err != nil {
		return err
	}

	toClose := []io.Closer{decompressedReader, src}
	return g.closeMany(toClose)
}

// CopyOut copies data from reader to writer while compressing it with Gzip. Then it closes the writer.
func (g GzipShovel) CopyOut(dst io.WriteCloser, src io.ReadCloser) error {
	compressionWriter := gzip.NewWriter(dst)

	if _, err := io.Copy(compressionWriter, src); err != nil {
		return err
	}

	toClose := []io.Closer{compressionWriter, dst}
	return g.closeMany(toClose)
}

func (g GzipShovel) closeMany(closers []io.Closer) error {
	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}
