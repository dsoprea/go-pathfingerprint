package pfinternal

import (
    "os"
    "io"
    "fmt"
    "hash"

    "path/filepath"
)

const (
    PathListBatchSize = 3
)

type Path struct {
    hashAlgorithm *string
}

func NewPath(hashAlgorithm *string) *Path {
    p := Path {
            hashAlgorithm: hashAlgorithm }

    return &p
}

func (self *Path) List(path *string) (<-chan *os.FileInfo, <-chan bool, error) {
    l := NewLogger("path")

    entriesChannel := make(chan *os.FileInfo)
    doneChannel := make(chan bool)

    go func() {
        f, err := os.Open(*path)
        l.DieIf(err, "Could not open list path")

        defer f.Close()

        for {
            entries, err := f.Readdir(PathListBatchSize)
            if err == io.EOF {
                break
            } else if err != nil {
                l.DieIf(err, "Could not list path children")
            }

            for i := range entries {
                entriesChannel <- &entries[i]
            }
        }

        doneChannel <- true
    }()

    return entriesChannel, doneChannel, nil
}

func (self *Path) getHashObject () (hash.Hash, error) {
    l := NewLogger("path")

    h, err := getHashObject(self.hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (path)", "hashAlgorithm", *self.hashAlgorithm)
        return nil, errorNew
    }

    return h, nil
}

func (self *Path) GeneratePathHash(scanPath *string, existingCatalog *Catalog) (string, error) {
    l := NewLogger("path")

    l.Debug("Generating hash for PATH.", "scanPath", *scanPath)

    f, err := os.Open(*scanPath)
    if err != nil {
        newError := l.MergeAndLogError(err, "Could not open scan-path", "scanPath", *scanPath)
        return "", newError
    }

    f.Close()

    if existingCatalog != nil {
        err := existingCatalog.Open()
        if err != nil {
            newError := l.MergeAndLogError(err, "Could not open catalog")
            return "", newError
        }

        closeCatalog := func () {
            l.Debug("Closing catalog.", "scanPath", *scanPath)

            existingCatalog.PruneOld()

            err = existingCatalog.Close()
            if err != nil {
                l.MergeAndLogError(err, "Could not close catalog")
                return
            }
        }

        defer closeCatalog()
    }

    p := NewPath(self.hashAlgorithm)
    entriesChannel, doneChannel, err := p.List(scanPath)
    if err != nil {
        newError := l.MergeAndLogError(err, "Could not list children in path", "scanPath", *scanPath)
        return "", newError
    }

    h, err := self.getHashObject()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (generate-path-hash)")
        return "", errorNew
    }

    done := false

    for done == false {
        select {
            case entry := <-entriesChannel:
                var childHash string = ""

                filename := (*entry).Name()
                isDir := (*entry).IsDir()

                childPath := filepath.Join(*scanPath, filename)

                if isDir == true {
                    var bc *Catalog

                    if existingCatalog != nil {
                        bc, err = existingCatalog.BranchCatalog(&filename)
                        if err != nil {
                            newError := l.MergeAndLogError(err, "Could not branch catalog", "catalogFilepath", *existingCatalog.GetCatalogFilepath(), "scanPath", childPath)
                            return "", newError
                        }
                    }

                    childHash, err = self.GeneratePathHash(&childPath, bc)
                    if err != nil {
                        newError := l.MergeAndLogError(err, "Could not generate PATH hash", "childPath", childPath, "catalogFilepath", *bc.GetCatalogFilepath())
                        return "", newError
                    }
                } else {
                    var lr *LookupResult

                    s, err := os.Stat(childPath)
                    if err != nil {
                        newError := l.MergeAndLogError(err, "Could not stat child file", "childPath", childPath)
                        return "", newError
                    }

                    mtime := s.ModTime().Unix()

                    if existingCatalog != nil {
                        lr, err = existingCatalog.Lookup(&filename)
                        if err != nil {
                            newError := l.MergeAndLogError(err, "Could not lookup filename hash", "catalogFilepath", *existingCatalog.GetCatalogFilepath(), "filename", filename, "mtime", mtime)
                            return "", newError
                        } else if lr.WasFound == false || lr.entry.mtime != mtime {
                            childHash = ""
                        } else {
                            childHash = lr.entry.hash
                        }
                    }

                    if childHash == "" {
                        childHash, err = self.GenerateFileHash(&childPath)
                        if err != nil {
                            newError := l.MergeAndLogError(err, "Could not generate FILE hash", "catalogFilepath", *existingCatalog.GetCatalogFilepath(), "childPath", childPath)
                            return "", newError
                        }
                    }

                    if existingCatalog != nil {
                        err = existingCatalog.Update(lr, mtime, &childHash)
                        if err != nil {
                            newError := l.MergeAndLogError(err, "Could not update catalog", "catalogFilepath", *existingCatalog.GetCatalogFilepath(), "childPath", childPath)
                            return "", newError
                        }
                    }
                }

                io.WriteString(h, childPath)
                io.WriteString(h, "\000")
                io.WriteString(h, childHash)
                io.WriteString(h, "\000")

            case done = <-doneChannel:
                break
        }
    }

    hash := fmt.Sprintf("%x", h.Sum(nil))
    l.Debug("Calculated PATH hash.", "scanPath", *scanPath, "hash", hash)

    return hash, nil
}

func (self *Path) GenerateFileHash(filepath *string) (string, error) {
    l := NewLogger("path")

    l.Debug("Generating hash for FILEPATH.", "filepath", *filepath)

    f, err := os.Open(*filepath)
    if err != nil {
        newError := l.MergeAndLogError(err, "Could not open filepath", "filepath", *filepath)
        return "", newError
    }

    closeFile := func () {
        l.Debug("Closing file (generated hash).", "filepath", *filepath)

        f.Close()
    }

    defer closeFile()

    h, err := self.getHashObject()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (generate-file-hash)")
        return "", errorNew
    }

    part := make([]byte, h.BlockSize() * 2)

    for {
        _, err := f.Read(part)
        if err == io.EOF {
            break
        } else if err != nil {
            newError := l.MergeAndLogError(err, "Could not read file part for hash", "filepath", *filepath)
            return "", newError
        }

        h.Write(part)
    }

    hash := fmt.Sprintf("%x", h.Sum(nil))
    l.Debug("Calculated FILE hash.", "hash", hash, "filepath", *filepath)

    return hash, nil
}
