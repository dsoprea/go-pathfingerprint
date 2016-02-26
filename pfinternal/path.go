package pfinternal

import (
    "os"
    "io"
    "fmt"
    "path"
    "hash"

    "io/ioutil"
)

const (
    PathListBatchSize = 3
)

type Path struct {
    hashAlgorithm *string
    reportingChannel chan<- *ChangeEvent
}

func NewPath(hashAlgorithm *string, reportingChannel chan<- *ChangeEvent) *Path {
    p := Path {
            hashAlgorithm: hashAlgorithm,
            reportingChannel: reportingChannel,
    }

    return &p
}

func (self *Path) getHashObject() (h hash.Hash, err error) {
    l := NewLogger("path")

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

// Generate a hash for a path.
func (self *Path) GeneratePathHash(scanPath *string, relPath *string, existingCatalog *Catalog) (hash string, err error) {
    l := NewLogger("path")

    defer func() {
        if r := recover(); r != nil {
            hash = ""
            err = r.(error)

            l.Error("Could not generate path hash", "err", err)
        }
    }()

    l.Debug("Generating hash for PATH.", "relPath", *relPath)

    h, err := self.getHashObject()
    if err != nil {
        panic(err)
    }

    // We need this list to be sorted (read: complete) in order to produce 
    // deterministic results.
    entries, err := ioutil.ReadDir(*scanPath)
    if err != nil {
        panic(err)
    }

    for _, entry := range entries {
        var childHash string = ""

        filename := entry.Name()
        isDir := entry.IsDir()

        childPath := path.Join(*scanPath, filename)
        relChildPath := path.Join(*relPath, filename)

        if isDir == true {
            var bc *Catalog

            bc, err = existingCatalog.BranchCatalog(&filename)
            if err != nil {
                panic(err)
            }

            childHash, err = self.GeneratePathHash(&childPath, &relChildPath, bc)
            if err != nil {
                panic(err)
            }
        } else {
            s, err := os.Stat(childPath)
            if err != nil {
                panic(err)
            }

            mtime := s.ModTime().Unix()

            flr, err := existingCatalog.lookupFile(&filename)
            if err != nil {
                panic(err)
            } else if flr.wasFound == false || flr.entry.mtime != mtime {
                childHash, err = self.GenerateFileHash(&childPath)
                if err != nil {
                    panic(err)
                }

                if flr.wasFound == false || childHash != flr.entry.hash {
// TODO(dustin): !! How do we or should we emit update events for paths?
                    err = existingCatalog.setFile(flr, mtime, &childHash)
                    if err != nil {
                        panic(err)
                    }
                }
            } else {
                childHash = flr.entry.hash
            }
        }

        io.WriteString(h, relChildPath)
        io.WriteString(h, "\000")
        io.WriteString(h, childHash)
        io.WriteString(h, "\000")
    }

    hash = fmt.Sprintf("%x", h.Sum(nil))
    l.Debug("Calculated PATH hash.", 
        "relPath", *relPath, 
        "hash", hash)

// TODO(dustin): !! How do we or should we emit update events for paths?
    lhp := existingCatalog.getLastHash()

    if lhp == nil || *lhp != hash {
        err = existingCatalog.updatePath(&hash)
        if err != nil {
            panic(err)
        }
    }

    return hash, nil
}

func (self *Path) GenerateFileHash(filepath *string) (hash string, err error) {
    l := NewLogger("path")

    defer func() {
        if r := recover(); r != nil {
            hash = ""
            err = r.(error)

            l.Error("Could not generate hash", "err", err)
        }
    }()

    l.Debug("Generating hash for FILEPATH.", "filepath", *filepath)

    f, err := os.Open(*filepath)
    if err != nil {
        panic(err)
    }

    defer func() {
        l.Debug("Closing file (generated hash).", "filepath", *filepath)
        f.Close()
    }()

    h, err := self.getHashObject()
    if err != nil {
        panic(err)
    }

    part := make([]byte, h.BlockSize() * 2)

    for {
        _, err := f.Read(part)
        if err == io.EOF {
            break
        } else if err != nil {
            panic(err)
        }

        h.Write(part)
    }

    hash = fmt.Sprintf("%x", h.Sum(nil))
    l.Debug("Calculated FILE hash.", 
        "hash", hash, 
        "filepath", *filepath)

    return hash, nil
}
