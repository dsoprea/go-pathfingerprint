package pfinternal

import (
    "os"
    "path"
    "io"
    "errors"
    "time"
//    "reflect"
)

const (
    DbType = "sqlite3"
    CatalogPathListBatchSize = 3
    RootCatalogFilename = "root"
)

var ErrNoHash = errors.New("no hash recorded for the filename")
var ErrFileChanged = errors.New("mtime for filename does not match")

type Catalog struct {
    catalogPath *string
    scanPath *string
    allowUpdates bool
    relScanPath *string
    catalogFilename *string
    catalogFilepath *string
    nowTime time.Time
    nowEpoch int64
    reportingChannel chan<- *ChangeEvent
    cc *catalogCommon
    cr *catalogResource
}

func NewCatalog (catalogPath *string, scanPath *string, allowUpdates bool, hashAlgorithm *string, reportingChannel chan<- *ChangeEvent) (*Catalog, error) {
    l := NewLogger("catalog")

    cc, err := newCatalogCommon(hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create catalog-common object (catalog)")
        return nil, errorNew
    }

    catalogFilename, err := cc.getCatalogFilename(nil)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not formulate catalog filename (catalog)")
        return nil, errorNew
    }

    catalogFilepath := path.Join(*catalogPath, *catalogFilename)

    nowTime := time.Now()
    nowEpoch := nowTime.Unix()

    l.Debug("Current time.", "nowEpoch", nowEpoch)

    // Keep this at nil. We just want it to be obvious how we're passing nil.
    var relScanPath *string

    cr, err := newCatalogResource(&catalogFilepath, relScanPath, hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create catalog-resource object (catalog)")
        return nil, errorNew
    }

    c := Catalog { 
            catalogPath: catalogPath, 
            scanPath: scanPath, 
            allowUpdates: allowUpdates,
            relScanPath: relScanPath, 
            catalogFilename: catalogFilename, 
            catalogFilepath: &catalogFilepath,
            nowTime: nowTime,
            nowEpoch: nowEpoch,
            reportingChannel: reportingChannel,
            cc: cc,
            cr: cr,
    }

    return &c, nil
}

func (self *Catalog) GetCatalogPath () *string {
    return self.catalogPath
}

func (self *Catalog) GetCatalogFilepath () *string {
    return self.catalogFilepath
}

func (self *Catalog) BranchCatalog (childPathName *string) (*Catalog, error) {
    var scanPath string

    l := NewLogger("catalog")
    
    scanPath = path.Join(*self.scanPath, *childPathName)

    var relScanPath string
    if self.relScanPath == nil {
        relScanPath = *childPathName
    } else {
        relScanPath = path.Join(*self.relScanPath, *childPathName)
    }

    catalogFilename, err := self.cc.getCatalogFilename(&relScanPath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not formulate catalog filename (branch catalog)")
        return nil, errorNew
    }

    catalogFilepath := path.Join(*self.catalogPath, *catalogFilename)

    cr, err := newCatalogResource(&catalogFilepath, &relScanPath, self.cc.HashAlgorithm())
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create catalog-resource object (branch catalog)")
        return nil, errorNew
    }

    c := Catalog { 
            catalogPath: self.catalogPath, 
            scanPath: &scanPath, 
            allowUpdates: self.allowUpdates,
            relScanPath: &relScanPath, 
            catalogFilename: catalogFilename, 
            catalogFilepath: &catalogFilepath,
            nowTime: self.nowTime,
            nowEpoch: self.nowEpoch,
            reportingChannel: self.reportingChannel,
            cc: self.cc,
            cr: cr,
    }

    return &c, nil
}

func (self *Catalog) Open () error {
    l := NewLogger("catalog")

    err := self.cr.Open()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not close catalog resource")
        return errorNew
    }

    return nil
}

func (self *Catalog) Close () error {
    l := NewLogger("catalog")

    err := self.cr.Close()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not close catalog resource")
        return errorNew
    }

    if self.allowUpdates == true {
        l.Debug("Updating catalog file times.", "catalogFilepath", *self.catalogFilepath)

        err := os.Chtimes(*self.catalogFilepath, self.nowTime, self.nowTime)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not update catalog times")
            return errorNew
        }
    }

    l.Debug("Catalog mtime updated.", "catalogFilepath", *self.catalogFilepath)

    return nil
}

func (self *Catalog) PruneOldCatalogs () error {
    l := NewLogger("catalog")

    l.Debug("Pruning old catalogs.")

    p, err := os.Open(*self.catalogPath)
    l.DieIf(err, "Could not open catalog path")

    defer p.Close()

    for {
        entries, err := p.Readdir(CatalogPathListBatchSize)
        if err == io.EOF {
            break
        } else if err != nil {
            l.DieIf(err, "Could not get next catalog")
        }

        for i := range entries {
            e := entries[i]
            mtimeTime := e.ModTime()
            mtimeEpoch := mtimeTime.Unix()
            catalogFilename := e.Name()
            catalogFilepath := path.Join(*self.catalogPath, catalogFilename)

            if mtimeEpoch >= self.nowEpoch {
                continue
            }

            l.Debug("Pruning catalog.", "catalogFilename", catalogFilename, "mtimeTime", mtimeTime, "mtimeEpoch", mtimeEpoch, "nowTime", self.nowTime, "nowEpoch", self.nowEpoch)

            // The catalog hasn't been touched. It must've been deleted.

            self.deleteCatalog(&catalogFilepath)
        }
    }

    l.Debug("Finished pruning old catalogs.")

    return nil
}

func (self *Catalog) deleteCatalog (catalogFilepath *string) error {
    l := NewLogger("catalog")

    doRelPathLookup := self.reportingChannel != nil
    relPath, err := deleteCatalog(catalogFilepath, self.cc.HashAlgorithm(), doRelPathLookup)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "There might have been an issue deleting the catalog", "catalogFilepath", *catalogFilepath)
        return errorNew
    }

    if relPath == nil {
        l.LogError("We couldn't read a relative-path from the catalog to be pruned, so we can't emit an event for it.", "catalogFilepath", *catalogFilepath)
    } else if self.reportingChannel != nil {
        self.reportingChannel <- &ChangeEvent { EntityType: &EntityTypePath, ChangeType: &UpdateTypeDelete, RelPath: relPath }
    }

    return nil
}

// Update the catalog with the info for the path that the catalog represents. 
// This action allows us to determine when the directory is new or when the 
// contents have changed. 
func (self *Catalog) SetPathHash (relPath *string, hash *string) (*string, error) {
    l := NewLogger("catalog")

    ps, err := self.cr.setPathHash(relPath, hash)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not set path hash (catalog)")
        return nil, errorNew
    }

    return ps, nil
}

func (self *Catalog) Lookup (filename *string) (*lookupResult, error) {
    l := NewLogger("catalog")

    var relScanPathPhrase string
    if self.relScanPath == nil {
        relScanPathPhrase = ""
    } else {
        relScanPathPhrase = *self.relScanPath
    }

    lr, err := self.cr.lookup(filename)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not lookup filename", "relScanPathPhrase", relScanPathPhrase)
        return nil, errorNew
    }

    if lr.WasFound == true && self.allowUpdates == true {
        // Update the timestamp of the record so that we can determine 
        // which records no longer represent valid files.

        l.Debug("Setting last_check_epoch for entry", "relScanPathPhrase", relScanPathPhrase, "filename", *filename, "last_check_epoch", self.nowEpoch)

        err = self.cr.updateLastCheck(lr.entry.id, self.nowEpoch)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not update last-check", "relScanPathPhrase", relScanPathPhrase, "filename", *filename)
            return nil, errorNew
        }
    }

    return lr, nil
}

func (self *Catalog) getFilePath (filename *string) string {
    var relFilepath string

    if self.relScanPath != nil {
        relFilepath = path.Join(*self.relScanPath, *filename)
    } else {
        relFilepath = *filename
    }

    return relFilepath
}

func (self *Catalog) Update (lr *lookupResult, mtime int64, hash *string) error {
    l := NewLogger("catalog")

    if lr.WasFound == true && lr.entry.mtime == mtime {
        // The entry already existed and the mtime matched.

        return nil
    }

    if self.reportingChannel != nil {
        relFilepath := self.getFilePath(lr.filename)

        if lr.WasFound == true {
            self.reportingChannel <- &ChangeEvent { EntityType: &EntityTypeFile, ChangeType: &UpdateTypeUpdate, RelPath: &relFilepath }
        } else {
            self.reportingChannel <- &ChangeEvent { EntityType: &EntityTypeFile, ChangeType: &UpdateTypeCreate, RelPath: &relFilepath }
        }
    }

    if self.allowUpdates == false {
        // We were told to not make any changes.

        return nil
    }

    err := self.cr.update(lr, mtime, hash, self.nowEpoch)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not update record", "catalogFilepath", self.catalogFilepath, "filename", lr.filename)
        return errorNew
    }

    return nil
}

// Delete all records that haven't been touched in this run (because all of the 
// ones that match known files have been updated to a later timestamp than they 
// had).
func (self *Catalog) PruneOldEntries () error {
    l := NewLogger("catalog")

    if self.allowUpdates == false {
        l.Debug("Not checking for deletions since we're not allowed to make updates.")
        return nil
    }

    if self.reportingChannel != nil {
        err := self.cr.getOld(self.nowEpoch, self.reportingChannel)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not report old file entries")
            return errorNew
        }
    }

    err := self.cr.pruneOld(self.nowEpoch)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prune old file entries")
        return errorNew
    }

    return nil
}
