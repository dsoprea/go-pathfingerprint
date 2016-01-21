package pfinternal

import (
    "os"
    "io"
    "fmt"
    "path"
    "hash"
    "io/ioutil"

    "path/filepath"
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

func (self *Path) getHashObject() (hash.Hash, error) {
    l := NewLogger("path")

    h, err := getHashObject(self.hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (path)", "hashAlgorithm", *self.hashAlgorithm)
        return nil, errorNew
    }

    return h, nil
}

func (self *Path) GeneratePathHash(scanPath *string, existingCatalog *Catalog) (string, error) {
    return self.generatePathHashInner(scanPath, nil, existingCatalog)
}

func (self *Path) generatePathHashInner(scanPath *string, relScanPath *string, existingCatalog *Catalog) (string, error) {
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

            existingCatalog.PruneOldEntries()

            err = existingCatalog.Close()
            if err != nil {
                l.MergeAndLogError(err, "Could not close catalog")
                return
            }
        }

        defer closeCatalog()
    }

    h, err := self.getHashObject()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (generate-path-hash)")
        return "", errorNew
    }

    entries, err := ioutil.ReadDir(*scanPath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not list path", "path", *scanPath)
        return "", errorNew
    }

    for _, entry := range entries {
        var childHash string = ""

        filename := entry.Name()
        isDir := entry.IsDir()

        childPath := filepath.Join(*scanPath, filename)

        var relChildPath string

        if relScanPath != nil {
            relChildPath = filepath.Join(*relScanPath, filename)
        } else {
            relChildPath = filename
        }

        if isDir == true {
            var bc *Catalog

            if existingCatalog != nil {
                bc, err = existingCatalog.BranchCatalog(&filename)
                if err != nil {
                    newError := l.MergeAndLogError(err, "Could not branch catalog", "catalogFilepath", *existingCatalog.GetCatalogFilepath(), "scanPath", childPath)
                    return "", newError
                }
            }

            var childRelScanPath string
            if relScanPath == nil {
                childRelScanPath = filename
            } else {
                childRelScanPath = path.Join(*relScanPath, filename)
            }

            childHash, err = self.generatePathHashInner(&childPath, &childRelScanPath, bc)
            if err != nil {
                newError := l.MergeAndLogError(err, "Could not generate PATH hash", "childPath", childPath, "catalogFilepath", *bc.GetCatalogFilepath())
                return "", newError
            }
        } else {
            var lr *lookupResult

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

        l.Warning("Entry.", "relChildPath", relChildPath, "childHash", childHash)

        io.WriteString(h, relChildPath)
        io.WriteString(h, "\000")
        io.WriteString(h, childHash)
        io.WriteString(h, "\000")
    }

    hash := fmt.Sprintf("%x", h.Sum(nil))
    l.Debug("Calculated PATH hash.", "scanPath", *scanPath, "hash", hash)

    var relScanPathNormalized string
    if relScanPath == nil {
        relScanPathNormalized = ""
    } else {
        relScanPathNormalized = *relScanPath
    }

    pathState, err := existingCatalog.SetPathHash(&relScanPathNormalized, &hash)
    if err != nil {
        newError := l.MergeAndLogError(err, "Could not update catalog path-info", "scanPath", *scanPath)
        return "", newError
    }

    if self.reportingChannel != nil {
        if *pathState == PathStateNew {
            self.reportingChannel <- &ChangeEvent { EntityType: &EntityTypePath, ChangeType: &UpdateTypeCreate, RelPath: &relScanPathNormalized }
        } else if (*pathState == PathStateUpdated) {
            self.reportingChannel <- &ChangeEvent { EntityType: &EntityTypePath, ChangeType: &UpdateTypeUpdate, RelPath: &relScanPathNormalized }
        }
    }

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
