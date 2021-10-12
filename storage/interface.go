package storage

import (
    "net/url"
)

type FileStorage interface {
    Read(p []byte) (n int, err error)
    Write(p []byte) (n int, err error)
    Close() error
}

func GetFileStorage(uri url.URL) FileStorage {
    switch uri.Scheme {
    case "":
        return getLocalFileStorage(uri)
    case "s3":
        return getS3FileStorage(uri)
    default:
        panic("Can not handle this uri")
    }
}
