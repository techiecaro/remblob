package storage

type FileStorage interface {
    Read(p []byte) (n int, err error)
    // Write(p []byte) (n int, err error)
    Close() error
}

func GetFileStorage(uri string) FileStorage {
    return getLocalFileStorage(uri)
}
