package core

import (
	"net/url"
	"path"
	"strings"
)

const gzipSuffix = ".gz"

// IsCompressed checks should the filename go though the compression/decompression
func isCompressed(fileURL url.URL) bool {
	return path.Ext(fileURL.String()) == gzipSuffix
}

func getBaseName(fileURL url.URL) string {
	baseName := path.Base(fileURL.String())
	if isCompressed(fileURL) {
		baseName = strings.TrimSuffix(baseName, gzipSuffix)
	}
	return baseName
}
