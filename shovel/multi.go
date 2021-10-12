package shovel

import "io"

// A MultiShovel copies between reader and writer. Both uncompressed and compressed use cases are permitted
type MultiShovel struct {
    SourceCompressed      bool
    DestinationCompressed bool
}

// CopyIn copies data from reader to writer while uncompressing it with Gzip if flagged. Then it closes the reader.
func (m MultiShovel) CopyIn(dst io.WriteCloser, src io.ReadCloser) error {
    var shovel Shovel
    if !m.SourceCompressed {
        shovel = PlainShovel{}
    } else {
        shovel = GzipShovel{}
    }
    return shovel.CopyIn(dst, src)
}

// CopyOut copies data from reader to writer while compressing it with Gzip it if flagged. Then it closes the writer.
func (m MultiShovel) CopyOut(dst io.WriteCloser, src io.ReadCloser) error {
    var shovel Shovel
    if !m.DestinationCompressed {
        shovel = PlainShovel{}
    } else {
        shovel = GzipShovel{}
    }
    return shovel.CopyOut(dst, src)
}
