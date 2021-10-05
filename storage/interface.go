package storage

import (
    "fmt"
    "net/url"
)

type FileStorage interface {
    Read(p []byte) (n int, err error)
    Write(p []byte) (n int, err error)
    Close() error
}

func GetFileStorage(uri url.URL) FileStorage {
    fmt.Println("Scheme: ", uri.Scheme)
    fmt.Println("Path: ", uri.Path)
    fmt.Println("Host: ", uri.Host)

    switch uri.Scheme {
    case "":
        return getLocalFileStorage(uri)
    case "s3":
        return getS3FileStorage(uri)
    default:
        panic("Can not handle this uri")
    }
}
