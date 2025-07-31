package shovel

import "io"

// shovelType represents the type of shovel to use
type shovelType int

const (
	shovelTypePlain shovelType = iota
	shovelTypeGzip
	shovelTypeParquet
)

// A MultiShovel copies between reader and writer. Handles compression and format conversion
type MultiShovel struct {
	SourceCompressed      bool
	DestinationCompressed bool
	SourceParquet         bool
	DestinationParquet    bool

	// shovelInstance keeps shovel instance for state preservation (especially for parquet)
	shovelInstance Shovel
}

// determineShovelType determines which shovel type to use based on compression and format flags
func (m *MultiShovel) determineShovelType(isSource bool) shovelType {
	if isSource {
		if m.SourceParquet {
			return shovelTypeParquet
		} else if m.SourceCompressed {
			return shovelTypeGzip
		}
	} else {
		if m.DestinationParquet {
			return shovelTypeParquet
		} else if m.DestinationCompressed {
			return shovelTypeGzip
		}
	}
	return shovelTypePlain
}

// createShovel creates a new shovel instance of the specified type
func createShovel(shovelType shovelType) Shovel {
	switch shovelType {
	case shovelTypeParquet:
		return &ParquetShovel{}
	case shovelTypeGzip:
		return GzipShovel{}
	default:
		return PlainShovel{}
	}
}

// CopyIn copies data from reader to writer while handling format conversion and decompression. Then it closes the reader.
func (m *MultiShovel) CopyIn(dst io.WriteCloser, src io.ReadCloser) error {
	shovelType := m.determineShovelType(true) // true for source
	m.shovelInstance = createShovel(shovelType)
	return m.shovelInstance.CopyIn(dst, src)
}

// CopyOut copies data from reader to writer while handling format conversion and compression. Then it closes the writer.
func (m *MultiShovel) CopyOut(dst io.WriteCloser, src io.ReadCloser) error {
	destinationType := m.determineShovelType(false) // false for destination
	sourceShovelType := m.determineShovelType(true) // true for source

	var shovel Shovel

	// Reuse existing shovel instance if types match and we have one from CopyIn
	if m.shovelInstance != nil && destinationType == sourceShovelType {
		shovel = m.shovelInstance
	} else {
		// Create new shovel for destination type
		shovel = createShovel(destinationType)
	}

	return shovel.CopyOut(dst, src)
}
