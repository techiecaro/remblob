package shovel

import "io"

// A MultiShovel copies between reader and writer. Handles compression and format conversion
type MultiShovel struct {
	SourceCompressed      bool
	DestinationCompressed bool
	SourceParquet         bool
	DestinationParquet    bool
}

// CopyIn copies data from reader to writer while handling format conversion and decompression. Then it closes the reader.
func (m MultiShovel) CopyIn(dst io.WriteCloser, src io.ReadCloser) error {
	var shovel Shovel

	// Handle parquet format first (takes precedence over compression)
	if m.SourceParquet {
		shovel = &ParquetShovel{}
	} else if !m.SourceCompressed {
		shovel = PlainShovel{}
	} else {
		shovel = GzipShovel{}
	}
	return shovel.CopyIn(dst, src)
}

// CopyOut copies data from reader to writer while handling format conversion and compression. Then it closes the writer.
func (m MultiShovel) CopyOut(dst io.WriteCloser, src io.ReadCloser) error {
	var shovel Shovel

	// Handle parquet format first (takes precedence over compression)
	if m.DestinationParquet {
		shovel = &ParquetShovel{}
	} else if !m.DestinationCompressed {
		shovel = PlainShovel{}
	} else {
		shovel = GzipShovel{}
	}
	return shovel.CopyOut(dst, src)
}
