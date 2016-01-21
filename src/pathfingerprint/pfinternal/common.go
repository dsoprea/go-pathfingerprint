package pfinternal

import (
    "fmt"
    "hash"
)

type catalogCommon struct {
    hashAlgorithm *string
}

func newCatalogCommon (hashAlgorithm *string) (*catalogCommon, error) {
    cc := catalogCommon {
        hashAlgorithm: hashAlgorithm,
    }
    
    return &cc, nil
}

func (self *catalogCommon) HashAlgorithm () *string {
    return self.hashAlgorithm
}

func (self *catalogCommon) getHashObject () (hash.Hash, error) {
    l := NewLogger("catalog_common")
    
    h, err := getHashObject(self.hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (catalog)", "hashAlgorithm", *self.hashAlgorithm)
        return nil, errorNew
    }

    return h, nil
}

func (self *catalogCommon) getCatalogFilename (relScanPath *string) (*string, error) {
    l := NewLogger("catalog_common")

    if relScanPath == nil {
        filename := RootCatalogFilename
        return &filename, nil
    } else {

        h, err := self.getHashObject()
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not get hash object (getCatalogFilename)")
            return nil, errorNew
        }

        h.Write([]byte(*relScanPath))

        catalogFilename := fmt.Sprintf("%x", h.Sum(nil))
        return &catalogFilename, nil
    }
}
