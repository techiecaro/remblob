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

func GetFileStorage(uri string) FileStorage {
    parsedURL, err := url.Parse(uri)
    if err != nil {
        panic(err)
    }
    fmt.Println("Scheme: ", parsedURL.Scheme)
    fmt.Println("Path: ", parsedURL.Path)
    fmt.Println("Host: ", parsedURL.Host)

    switch parsedURL.Scheme {
    case "":
        return getLocalFileStorage(uri)
    case "s3":
        return getS3FileStorage(uri)
    default:
        panic("Can not handle this uri")
    }
}
