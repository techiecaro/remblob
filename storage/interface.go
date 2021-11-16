package storage

import (
    "log"
    "net/url"
    "sort"
)

type FileStorage interface {
    Read(p []byte) (n int, err error)
    Write(p []byte) (n int, err error)
    Close() error
}

type fileStorageBuilder func(url.URL) FileStorage
type FileLister func(url.URL) []url.URL

type registrationInfo struct {
    storage           fileStorageBuilder
    lister            FileLister
    prefixes          []string
    completionPrompts []string
}

// fileStorageRegister registers available implementations.
var fileStorageRegister = make(map[string]registrationInfo)

func registerFileStorage(registration registrationInfo) {
    for _, prefix := range registration.prefixes {
        uriPrefix, err := url.Parse(prefix)
        if err != nil {
            log.Fatalf("Registration of %s can't progress. Can't parse it", prefix)
        }

        if _, ok := fileStorageRegister[uriPrefix.Scheme]; ok {
            log.Fatalf("FileStorage with scheme %s already registered", uriPrefix.Scheme)
        }
        fileStorageRegister[uriPrefix.Scheme] = registration
    }
}

func EmptyFileLister(prefix url.URL) []url.URL {
    return []url.URL{}
}

func GetFileStorage(uri url.URL) FileStorage {
    if info, ok := fileStorageRegister[uri.Scheme]; ok {
        return info.storage(uri)
    }

    panic("Can not handle this uri")
}

func GetFileListerPrefixes() []string {
    uniquePrefixes := map[string]bool{}
    for _, info := range fileStorageRegister {
        for _, prefix := range info.prefixes {
            if prefix == "" {
                continue
            }
            uniquePrefixes[prefix] = true
        }
        for _, prompt := range info.completionPrompts {
            uniquePrefixes[prompt] = true
        }
    }

    keys := make([]string, len(uniquePrefixes))
    i := 0
    for prefix := range uniquePrefixes {
        keys[i] = prefix
        i++
    }

    sort.Strings(keys)
    return keys
}

func GetFileLister(prefix url.URL) FileLister {
    lister := EmptyFileLister

    if info, ok := fileStorageRegister[prefix.Scheme]; ok {
        if info.lister != nil {
            lister = info.lister
        }
    }

    return lister
}
