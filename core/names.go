package core

import (
	"net/url"
	"path"
	"strings"
)

const gzipSuffix = ".gz"
const parquetSuffix = ".parquet"

// IsCompressed checks should the filename go though the compression/decompression
func isCompressed(fileURL url.URL) bool {
	return path.Ext(fileURL.String()) == gzipSuffix
}

// IsParquet checks if the filename is a parquet file
func isParquet(fileURL url.URL) bool {
	return path.Ext(fileURL.String()) == parquetSuffix
}

func getBaseName(fileURL url.URL) string {
	baseName := path.Base(fileURL.String())
	if isCompressed(fileURL) {
		baseName = strings.TrimSuffix(baseName, gzipSuffix)
	}
	if isParquet(fileURL) {
		// For parquet files, change extension to .csv for editing
		baseName = strings.TrimSuffix(baseName, parquetSuffix) + ".csv"
	}
	return baseName
}
