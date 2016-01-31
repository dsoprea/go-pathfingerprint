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

func (self *catalogCommon) getHashObject () (hash.Hash, error) {
    l := NewLogger("catalog_common")
    
    h, err := getHashObject(self.hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (catalog)", "hashAlgorithm", *self.hashAlgorithm)
        return nil, errorNew
    }

    return h, nil
}
