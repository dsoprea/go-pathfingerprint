package pfinternal

import (
    "path"
    "errors"
    "time"
)

const (
    DbType = "sqlite3"
//    CatalogPathListBatchSize = 3
)

// TODO(dustin): !! We ended-up going the other direction with our errors. Deimplement these?
var ErrNoHash = errors.New("no hash recorded for the filename")
var ErrFileChanged = errors.New("mtime for filename does not match")

type Catalog struct {
    scanPath string
    allowUpdates bool
    lastHash *string

    pd pathDescriptor
    nowTime time.Time
    nowEpoch int64
    reportingChannel chan<- *ChangeEvent
    cc *catalogCommon
    cr *catalogResource
}

func NewCatalog(catalogResource *catalogResource, scanPath *string, allowUpdates bool, hashAlgorithm *string, reportingChannel chan<- *ChangeEvent) (cp *Catalog, err error) {
    l := NewLogger("catalog")

    defer func() {
        if r := recover(); r != nil {
            cp = nil
            err = r.(error)
            l.Error("Could not create catalog.", "err", err)
        }
    }()

    l.Debug("Creating catalog.")

    cc, err := newCatalogCommon(hashAlgorithm)
    if err != nil {
        panic(err)
    }

    nowTime := time.Now()
    nowEpoch := nowTime.Unix()

    l.Debug("Current time.", "nowEpoch", nowEpoch)

    c := Catalog { 
            scanPath: *scanPath, 
            allowUpdates: allowUpdates,
            nowTime: nowTime,
            nowEpoch: nowEpoch,
            reportingChannel: reportingChannel,
            cc: cc,
            cr: catalogResource,
    }

    relPath := ""
    pdp, hash, err := c.ensurePathRecord(&relPath)
    if err != nil {
        panic(err)
    }

    c.pd = *pdp
    c.lastHash = hash

    return &c, nil
}

func (self *Catalog) BranchCatalog(childPathName *string) (*Catalog, error) {
    l := NewLogger("catalog")

    l.Debug("Branching catalog.", "childPathName", *childPathName)

    scanPath := path.Join(self.scanPath, *childPathName)
    relPath := path.Join(self.pd.GetRelPath(), *childPathName)

    pd, hash, err := self.ensurePathRecord(&relPath)
    if err != nil {
        panic(err)
    }

    c := Catalog { 
            scanPath: scanPath, 
            allowUpdates: self.allowUpdates,
            lastHash: hash,
            pd: *pd,
            nowTime: self.nowTime,
            nowEpoch: self.nowEpoch,
            reportingChannel: self.reportingChannel,
            cc: self.cc,
            cr: self.cr,
    }

    return &c, nil
}

func (self *Catalog) getLastHash() *string {
    return self.lastHash
}

func (self *Catalog) ensurePathRecord(relPath *string) (*pathDescriptor, *string, error) {
    l := NewLogger("catalog")

    l.Debug("Ensuring path record.", "relPath", *relPath)

    plr, err := self.lookupPath(relPath)
    if err != nil {
        panic(err)
    }

    var pd *pathDescriptor
    var hash *string

    if plr.wasFound == true {
        pd = newRecordedPathDescriptor(relPath, plr.entry.id)
        hash = &plr.entry.hash
    } else if self.allowUpdates == true {
        pathInfoId, err := self.createPath(relPath)
        if err != nil {
            panic(err)
        }

        pd = newRecordedPathDescriptor(relPath, pathInfoId)
    } else {
        pd = newUnknownPathDescriptor(relPath)
    }

    l.Debug("Path record ensured.", "relPath", *relPath)

    return pd, hash, nil
}

func (self *Catalog) Open() error {
    return nil
}

func (self *Catalog) Close() error {
    if self.allowUpdates == true {
        err := self.PruneOldFiles()
        if err != nil {
            panic(err)
        }

        err = self.PruneOldPaths()
        if err != nil {
            panic(err)
        }
    }

    return nil
}

func (self *Catalog) lookupFile(filename *string) (flrp *fileLookupResult, err error) {
    l := NewLogger("catalog")

    defer func() {
        if r := recover(); r != nil {
            flrp = nil
            err = r.(error)
            l.Error("Could not lookup file.", "err", err)
        }
    }()

    l.Debug("Looking up file.", 
        "relPath", self.pd.GetRelPath(), 
        "filename", *filename)

    if self.pd.GetPathInfoId() == 0 {
        // The path record wasn't even found and we weren't allowed to create 
        // it.

        flrp = newNotFoundFileLookupResult(&self.pd, filename)
    } else {
        flrp, err = self.cr.lookupFile(&self.pd, filename)
        if err != nil {
            panic(err)
        }

        if flrp.wasFound == true && self.allowUpdates == true {
            // Update the timestamp of the record so that we can determine 
            // which records no longer represent valid files.

            l.Debug("Setting last_check_epoch for entry", 
                "relPath", self.pd.GetRelPath(), 
                "filename", *filename, 
                "last_check_epoch", self.nowEpoch)

            err = self.cr.updateLastFileCheck(flrp, self.nowEpoch)
            if err != nil {
                panic(err)
            }
        }
    }

    l.Debug("File lookup result.", 
        "found", flrp.wasFound, 
        "entry", flrp.entry)

    return flrp, nil
}

func (self *Catalog) lookupPath(relPath *string) (plrp *pathLookupResult, err error) {
    l := NewLogger("catalog")

    defer func() {
        if r := recover(); r != nil {
            plrp = nil
            err = r.(error)
            l.Error("Could not lookup path.", "err", err)
        }
    }()

    l.Debug("Doing path lookup.", "relPath", *relPath)

    plrp, err = self.cr.lookupPath(relPath)
    if err != nil {
        panic(err)
    }

    if plrp.wasFound == true && self.allowUpdates == true {
        // Update the timestamp of the record so that we can determine 
        // which records no longer represent valid files.

        l.Debug("Setting last_check_epoch for path", 
            "relPath", *relPath, 
            "last_check_epoch", self.nowEpoch)

        err = self.cr.updateLastPathCheck(plrp, self.nowEpoch)
        if err != nil {
            panic(err)
        }
    }

    l.Debug("Path lookup finished.", "relPath", *relPath)

    return plrp, nil
}

func (self *Catalog) setFile(flrp *fileLookupResult, mtime int64, hash *string) (err error) {
    l := NewLogger("catalog")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not lookup set file info.", "err", err)
        }
    }()

    if self.reportingChannel != nil {
        relFilepath := path.Join(self.pd.GetRelPath(), flrp.filename)

        if flrp.wasFound == true {
            self.reportingChannel <- &ChangeEvent { 
                    EntityType: EntityTypeFile, 
                    ChangeType: UpdateTypeUpdate, 
                    RelPath: relFilepath,
            }
        } else {
            self.reportingChannel <- &ChangeEvent {
                    EntityType: EntityTypeFile,
                    ChangeType: UpdateTypeCreate,
                    RelPath: relFilepath,
            }
        }
    }

    if self.allowUpdates == true {
        err = self.cr.setFile(flrp, mtime, hash, self.nowEpoch)
        if err != nil {
            panic(err)
        }
    }

    return nil
}

// Create a new path record for the given path.
func (self *Catalog) createPath(relPath *string) (pathInfoId int, err error) {
    l := NewLogger("catalog")

    defer func() {
        if r := recover(); r != nil {
            pathInfoId = 0

            err = r.(error)
            l.Error("Could not create path.", "err", err)
        }
    }()

    if self.reportingChannel != nil {
        self.reportingChannel <- &ChangeEvent {
                EntityType: EntityTypePath,
                ChangeType: UpdateTypeCreate,
                RelPath: *relPath,
        }
    }

    // This should never come up.
    if self.allowUpdates == false {
        panic(errors.New("We can't create a path record. We're not allowed."))
    }

    pathInfoId, err = self.cr.createPath(relPath, self.nowEpoch)
    if err != nil {
        panic(err)
    }

    return pathInfoId, nil
}

// Update the path that this catalog object represents.
func (self *Catalog) updatePath(hash *string) (err error) {
    l := NewLogger("catalog")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not update path.", "err", err)
        }
    }()

    if self.reportingChannel != nil {
        self.reportingChannel <- &ChangeEvent {
                EntityType: EntityTypePath,
                ChangeType: UpdateTypeUpdate,
                RelPath: self.pd.GetRelPath(),
        }
    }

    if self.allowUpdates == false {
        // We were told to not make any changes.
        return nil
    }

    err = self.cr.updatePath(&self.pd, hash)
    if err != nil {
        panic(err)
    }

    return nil
}

// Delete all file records that haven't been touched in this run (because all 
// of the ones that match known files have been updated to a later timestamp 
// than they had).
func (self *Catalog) PruneOldFiles() (err error) {
    l := NewLogger("catalog")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not prune old files.", "err", err)
        }
    }()

    if self.allowUpdates == false {
        l.Debug("Not checking for FILE deletions since we're not allowed to make updates.")
        return nil
    }

    err = self.cr.pruneOldFiles(self.nowEpoch, self.reportingChannel)
    if err != nil {
        panic(err)
    }

    return nil
}

// Delete all path records that haven't been touched in this run (because all 
// of the ones that match known files have been updated to a later timestamp 
// than they had).
func (self *Catalog) PruneOldPaths() (err error) {
    l := NewLogger("catalog")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not prune old paths.", "err", err)
        }
    }()

    if self.allowUpdates == false {
        l.Debug("Not checking for PATH deletions since we're not allowed to make updates.")
        return nil
    }

    err = self.cr.pruneOldPaths(self.nowEpoch, self.reportingChannel)
    if err != nil {
        panic(err)
    }

    return nil
}
