package pfinternal

// Describes a relative-filepath and a corresponding database ID. The database 
// ID may be (0) if not present.
type pathDescriptor struct {
    relPath string
    pathInfoId int
}

func newRecordedPathDescriptor(relPath *string, pathInfoId int) *pathDescriptor {
    pd := pathDescriptor {
            relPath: *relPath,
            pathInfoId: pathInfoId,
    }

    return &pd
}

func newUnknownPathDescriptor(relPath *string) *pathDescriptor {
    pd := pathDescriptor {
            relPath: *relPath,
            pathInfoId: 0,
    }

    return &pd
}

func (self *pathDescriptor) GetRelPath() string {
    return self.relPath
}

// Return the ID recorded for the path. If it's not recorded, it will be (0).
func (self *pathDescriptor) GetPathInfoId() int {
    return self.pathInfoId
}

// Represents a file record in the DB.
// TODO(dustin): Rename to fileEntry.
type catalogEntry struct {
    id int
    hash string
    mtime int64
}

func newCatalogEntry(id int, hash *string, mtime int64) *catalogEntry {
    ce := catalogEntry {
            id: id,
            hash: *hash,
            mtime: mtime,
    }

    return &ce
}

// Represents the output of lookupFile()
type fileLookupResult struct {
    wasFound bool
    pd pathDescriptor
    filename string
    entry catalogEntry
}

func newNotFoundFileLookupResult(pd *pathDescriptor, filename *string) *fileLookupResult {
    flr := fileLookupResult {
            wasFound: false,
            pd: *pd,
            filename: *filename,
    }

    return &flr
}

func newFoundFileLookupResult(pd *pathDescriptor, filename *string, entry *catalogEntry) *fileLookupResult {
    flr := fileLookupResult {
            wasFound: true,
            pd: *pd,
            filename: *filename,
            entry: *entry,
    }

    return &flr
}

// Represents a path record in the DB.
type pathEntry struct {
    id int
    hash string
}

func newPathEntry(id int, hash *string) *pathEntry {
    pe := pathEntry {
            id: id,
            hash: *hash,
    }

    return &pe
}

// Represents the output of lookupPath()
type pathLookupResult struct {
    wasFound bool
    relPath string
    entry pathEntry
}

func newNotFoundPathLookupResult(relPath *string) *pathLookupResult {
    plr := pathLookupResult {
            wasFound: false,
            relPath: *relPath,
    }

    return &plr
}

func newFoundPathLookupResult(relPath *string, entry *pathEntry) *pathLookupResult {
    plr := pathLookupResult {
            wasFound: true,
            relPath: *relPath,
            entry: *entry,
    }

    return &plr
}
