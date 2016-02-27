package pfinternal

import (
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

func (self *catalogCommon) getHashObject () (h hash.Hash, err error) {
    l := NewLogger("catalog_common")

    defer func() {
        if r := recover(); r != nil {
            h = nil
            err = r.(error)

            l.Error("Could not get hash object", "err", err)
        }
    }()
    
    h, err = getHashObject(self.hashAlgorithm)
    if err != nil {
        panic(err)
    }

    return h, nil
}
