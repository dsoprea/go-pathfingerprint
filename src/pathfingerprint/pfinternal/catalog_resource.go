package pfinternal

import (
    "strings"
    "strconv"
    "os"
    "path"
    "path/filepath"
    "database/sql"

    _ "github.com/mattn/go-sqlite3"
)

type lookupResult struct {
    WasFound bool
    filename *string
    entry *catalogEntry
}

type catalogEntry struct {
    id int
    hash string
    mtime int64
}

type catalogResource struct {
    catalogFilepath *string
    relScanPath *string
    db *sql.DB
    cc *catalogCommon
}

func newCatalogResource (catalogFilepath *string, relScanPath *string, hashAlgorithm *string) (*catalogResource, error) {
    l := NewLogger("catalog_resource")

    cc, err := newCatalogCommon(hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create catalog-common object (catalog-resource)")
        return nil, errorNew
    }

    doLookup := relScanPath == nil
    if relScanPath != nil && *relScanPath == "" {
        relScanPath = nil
    }

    cr := catalogResource { 
            catalogFilepath: catalogFilepath,
            relScanPath: relScanPath,
            cc: cc,
    }

    // If no relScanPath was given, look it up in the actual catalog.
    if doLookup {
        l.Debug("We weren't given a relScanPath, so we'll need to look it up", "catalogFilepath", *catalogFilepath)

        err := cr.Open()
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not open catalog object (newCatalogResource)")
            return nil, errorNew
        }

        defer cr.Close()

        relScanPath, err := cr.getRelPath()
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not lookup relScanPath in the catalog that we were given (newCatalogResource)")
            return nil, errorNew
        }

        cr.relScanPath = relScanPath
    }

    return &cr, nil
}

// TODO(dustin): We need to be able to tell Open() to not make any changes (in 
//               no-updates mode).

func (self *catalogResource) Open () error {
    var err error
    var db *sql.DB

    l := NewLogger("catalog_resource")

    l.Debug("Opening catalog.", "catalogFilepath", *self.catalogFilepath)

    if self.db != nil {
        errorNew := l.LogError("Connection already opened.")
        return errorNew
    }

    // Open the DB.

    db, err = sql.Open(DbType, *self.catalogFilepath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not connect DB", "DbType", DbType, "DbFilename", self.catalogFilepath)
        return errorNew
    }

    // Make sure the table exists.

    h, err := self.cc.getHashObject()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (catalog-open)")
        return errorNew
    }

    query := 
        "CREATE TABLE `path_info` (" +
            "`path_info_id` INTEGER NOT NULL PRIMARY KEY, " +
            "`rel_path` VARCHAR(1000) NOT NULL, " +
            "`hash` VARCHAR(" + strconv.Itoa(h.Size() * 2) + ") NOT NULL, " +
            "`schema_version` INTEGER NOT NULL DEFAULT 1" +
        ")"

    err = self.createTable (db, "path_info", &query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create path_info table")
        return errorNew
    }

    query = 
        "CREATE TABLE `catalog_entries` (" +
            "`catalog_entry_id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, " +
            "`filename` VARCHAR(255) NOT NULL, " +
            "`hash` VARCHAR(" + strconv.Itoa(h.Size() * 2) + ") NOT NULL, " +
            "`mtime_epoch` INTEGER UNSIGNED NOT NULL, " +
            "`last_check_epoch` INTEGER UNSIGNED NULL DEFAULT 0, " +
            "CONSTRAINT `filename_idx` UNIQUE (`filename`)" +
        ")"

    err = self.createTable (db, "catalog_entries", &query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create catalog_entries table")
        return errorNew
    }

    self.db = db

    return nil
}

func (self *catalogResource) createTable (db *sql.DB, tableName string, tableQuery *string) error {
    l := NewLogger("catalog-resource")

    _, err := db.Exec(*tableQuery)

    if err != nil {
        // Check for something like this: table `catalog` already exists
        if strings.HasSuffix(err.Error(), "already exists") {
            l.Debug("Table already exists.", "Name", tableName)
        } else {
            errorNew := l.MergeAndLogError(err, "Could not create table")
            return errorNew
        }
    }

    return nil
}

func (self *catalogResource) Close () error {
    l := NewLogger("catalog-resource")

    l.Debug("Closing catalog.", "catalogFilepath", *self.catalogFilepath)

    if self.db == nil {
        errorNew := l.LogError("Connection not open and can't be closed.")
        return errorNew
    }

    self.db.Close()
    self.db = nil

    return nil
}

// Update the catalog with the info for the path that the catalog represents. 
// This action allows us to determine when the directory is new or when the 
// contents have changed. 
func (self *catalogResource) setPathHash (relPath *string, hash *string) (*string, error) {
    l := NewLogger("catalog-resource")

    l.Debug("Updating path hash.", "catalogFilepath", *self.catalogFilepath, "relPath", *relPath, "hash", *hash)

    query := 
        "SELECT " +
            "`pi`.`hash` " +
        "FROM " +
            "`path_info` `pi` " +
        "WHERE " +
            "`pi`.`path_info_id` = 1"

    rows, err := self.db.Query(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare path-info lookup query")
        return nil, errorNew
    }

    if rows.Next() == true {
        // The record exists. Check the hash value.

        var currentHash string

        l.Debug("Updating existing path-info record.", "catalogFilepath", *self.catalogFilepath, "relPath", *relPath)

        err = rows.Scan(&currentHash)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not parse path-info lookup result record")
            return nil, errorNew
        }

        rows.Close()

        if currentHash == *hash {
            return &PathStateUnaffected, nil
        }

        // The hash has changed.

// TODO(dustin): Can we use an alias on the table here?
        query := 
            "UPDATE " +
                "`path_info` " +
            "SET " +
                "`hash` = ? " +
            "WHERE " +
                "`path_info_id` = 1"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not prepare path-info UPDATE query")
            return nil, errorNew
        }

        _, err = stmt.Exec(*hash)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not execute path-info UPDATE query")
            return nil, errorNew
        }

        l.Debug("Path-info has been updated.", "catalogFilepath", *self.catalogFilepath)

        return &PathStateUpdated, nil
    } else {
        // The record doesn't exist. Create it.

        l.Debug("Inserting path-info record.", "catalogFilepath", *self.catalogFilepath, "relPath", *relPath)

        query := 
            "INSERT INTO `path_info` " +
                "(`path_info_id`, `rel_path`, `hash`) " +
            "VALUES " +
                "(1, ?, ?)"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not prepare path-info INSERT query")
            return nil, errorNew
        }

        _, err = stmt.Exec(*relPath, *hash)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not execute path-info INSERT query")
            return nil, errorNew
        }

        l.Debug("Path-info has been inserted.", "catalogFilepath", *self.catalogFilepath)
    
        return &PathStateNew, nil
    }
}

func (self *catalogResource) lookup (filename *string) (*lookupResult, error) {
    var err error
    var lr lookupResult

    l := NewLogger("catalog-resource")

    query := 
        "SELECT " +
            "`ce`.`catalog_entry_id`, " +
            "`ce`.`hash`, " +
            "`ce`.`mtime_epoch` " +
        "FROM " +
            "`catalog_entries` `ce` " +
        "WHERE " +
            "`filename` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare lookup query")
        return nil, errorNew
    }

    rows, err := stmt.Query(filename)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute lookup")
        return nil, errorNew
    }

    defer rows.Close()

    var relScanPathPhrase string
    if self.relScanPath == nil {
        relScanPathPhrase = ""
    } else {
        relScanPathPhrase = *self.relScanPath
    }

    if rows.Next() == false {
        // We don't yet know about this file.

        l.Debug("Filename not yet in catalog", "relScanPath", relScanPathPhrase, "filename", *filename)

        lr.WasFound = false
        lr.filename = filename
    } else {
        // We already know about this file.

        l.Debug("Filename IS ALREADY in catalog", "relScanPath", relScanPathPhrase, "filename", *filename)

        var entry catalogEntry

        err = rows.Scan(&entry.id, &entry.hash, &entry.mtime)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not parse lookup result record")
            return nil, errorNew
        }

        lr.WasFound = true
        lr.filename = filename
        lr.entry = &entry
    }

    return &lr, nil
}

func (self *catalogResource) updateLastCheck (id int, nowEpoch int64) error {
    l := NewLogger("catalog-resource")

// TODO(dustin): Can we use an alias on the table here?
    query := 
        "UPDATE " +
            "`catalog_entries` " +
        "SET " +
            "`last_check_epoch` = ? " +
        "WHERE " +
            "`catalog_entry_id` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare found-update query")
        return errorNew
    }

    r, err := stmt.Exec(nowEpoch, id)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute found-update query")
        return errorNew
    }

    affected, err := r.RowsAffected()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get the number of affected rows from the found-update query")
        return errorNew
    }

    l.Debug("Epoch updated.", "id", id, "affected", affected)

    if affected < 1 {
        errorNew := l.LogError("No rows were affected by the found-update query")
        return errorNew
    }

    return nil
}

func (self *catalogResource) update (lr *lookupResult, mtime int64, hash *string, nowEpoch int64) error {
    l := NewLogger("catalog-resource")

    if lr.WasFound == true {
        l.Debug("Updating entry", "filename", *lr.filename, "id", lr.entry.id, "mtime", lr.entry.mtime, "hash", lr.entry.hash)

// TODO(dustin): Can we use an alias on the table here?
        query := 
            "UPDATE " +
                "`catalog_entries` " +
            "SET " +
                "`hash` = ?, " +
                "`mtime_epoch` = ? " +
            "WHERE " +
                "`catalog_entry_id` = ?"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not prepare entry-update query")
            return errorNew
        }

        r, err := stmt.Exec(hash, mtime, lr.entry.id)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not execute entry-update query")
            return errorNew
        }

        affected, err := r.RowsAffected()
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not get the number of affected rows from the entry-update query")
            return errorNew
        }

        if affected < 1 {
            return l.LogError("No rows were affected by the entry-update query")
        } else if affected > 1 {
            return l.LogError("Too many rows were affected by the entry-update query")
        }
    } else {
        // The filename wasn't in the catalog. Add it.

        l.Debug("Inserting entry", "filename", *lr.filename, "mtime", mtime, "hash", *hash, "last_check_epoch", nowEpoch)

        query := 
            "INSERT INTO `catalog_entries` " +
                "(`filename`, `hash`, `mtime_epoch`, `last_check_epoch`) " +
            "VALUES " +
                "(?, ?, ?, ?)"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not prepare entry-insert query")
            return errorNew
        }

        _, err = stmt.Exec(lr.filename, hash, mtime, nowEpoch)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not execute entry-insert query")
            return errorNew
        }
    }

    return nil
}

func (self *catalogResource) getFilePath (filename *string) string {
    var relFilepath string

    if self.relScanPath != nil {
        relFilepath = path.Join(*self.relScanPath, *filename)
    } else {
        relFilepath = *filename
    }

    return relFilepath
}

// Get a list of all records that haven't been touched in this run (because all 
// of the ones that match known files have been updated to a later timestamp 
// than they had).
func (self *catalogResource) getOld (nowEpoch int64, c chan<- *ChangeEvent) error {
    l := NewLogger("catalog-resource")

    // If we're reporting changes, then enumerate the entries to be delete 
    // and push them up.

    query := 
        "SELECT " +
            "`ce`.`filename` " +
        "FROM " +
            "`catalog_entries` `ce` " +
        "WHERE " +
            "`ce`.`last_check_epoch` < ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare pruned-entries query")
        return errorNew
    }

    rows, err := stmt.Query(nowEpoch)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute pruned-entries query")
        return errorNew
    }

    defer rows.Close()

    for rows.Next() {
        var filename string

        err = rows.Scan(&filename)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not parse filename from pruned-entries query")
            return errorNew
        }

        relFilepath := self.getFilePath(&filename)
        c <- &ChangeEvent { 
                EntityType: &EntityTypeFile, 
                ChangeType: &UpdateTypeDelete, 
                RelPath: &relFilepath,
        }
    }

    return nil
}

// Delete all records that haven't been touched in this run (because all of the 
// ones that match known files have been updated to a later timestamp than they 
// had).
func (self *catalogResource) pruneOld (nowEpoch int64) error {
    l := NewLogger("catalog-resource")

    query := 
        "DELETE FROM `catalog_entries` " +
        "WHERE " +
            "`last_check_epoch` < ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare entries-prune query")
        return errorNew
    }

    r, err := stmt.Exec(nowEpoch)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute entries-prune query")
        return errorNew
    }

    affected, err := r.RowsAffected()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get the number of affected rows from the entries-prune query")
        return errorNew
    }

    l.Debug("Pruned old entries.", "affected", affected)

    return nil
}

func (self *catalogResource) getRelPath () (*string, error) {
    var relPath string

    l := NewLogger("catalog-resource")

    query := 
        "SELECT " +
            "`pi`.`rel_path` " +
        "FROM " +
            "`path_info` `pi` " +
        "WHERE " +
            "`pi`.`path_info_id` = 1"

    rows, err := self.db.Query(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare path-info identification query (getRelPath)", "catalogFilepath", *self.catalogFilepath)
        return nil, errorNew
    }

    defer rows.Close()

    if rows.Next() == false {
        errorNew := l.LogError("Path-info identification result was erroneously empty (getRelPath).", "catalogFilepath", *self.catalogFilepath)
        return nil, errorNew
    }

    err = rows.Scan(&relPath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not parse path-info identification result record (getRelPath)", "catalogFilepath", *self.catalogFilepath)
        return nil, errorNew
    }

    return &relPath, nil
}

func (self *catalogResource) getLastPathHash () (*string, error) {
    var hash string

    l := NewLogger("catalog-resource")

    query := 
        "SELECT " +
            "`pi`.`hash` " +
        "FROM " +
            "`path_info` `pi` " +
        "WHERE " +
            "`pi`.`path_info_id` = 1"

    rows, err := self.db.Query(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare path-info identification query (getLastPathHash)")
        return nil, errorNew
    }

    defer rows.Close()

    if rows.Next() == false {
        errorNew := l.LogError("Path-info identification result was erroneously empty (getLastPathHash).", "catalogFilepath", *self.catalogFilepath)
        return nil, errorNew
    }

    err = rows.Scan(&hash)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not parse path-info identification result record (getLastPathHash)", "catalogFilepath", *self.catalogFilepath)
        return nil, errorNew
    }

    return &hash, nil
}

type CatalogCallback func (*catalogResource) error

func executeWithCatalog (catalogFilepath *string, hashAlgorithm *string, cb CatalogCallback) error {
    l := NewLogger("catalog-resource")

    cr, err := newCatalogResource(catalogFilepath, nil, hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create catalog-resource object (executeWithCatalog)")
        return errorNew
    }

    err = cr.Open()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not open catalog object (executeWithCatalog)")
        return errorNew
    }

    defer cr.Close()

    cbErr := cb(cr)
    if cbErr != nil {
        errorNew := l.MergeAndLogError(cbErr, "Catalog callback failed (executeWithCatalog)")
        return errorNew
    }

    return nil
}

func deleteCatalog (catalogFilepath *string, hashAlgorithm *string, doLookupRelPath bool) (*string, error) {
    var relPath *string

    l := NewLogger("catalog-resource")

    if doLookupRelPath == true {
        l.Debug("Reading rel-path of catalog-to-be-pruned.", "catalogFilepath", *catalogFilepath)

        cb := func (cr *catalogResource) error {
            var err error

            relPath, err = cr.getRelPath()
            if err != nil {
                errorNew := l.MergeAndLogError(err, "Could not lookup path for catalog", "catalogFilepath", *catalogFilepath)
                return errorNew
            }

            return nil
        }

        err := executeWithCatalog(catalogFilepath, hashAlgorithm, cb)
        if err != nil {
            l.MergeAndLogError(err, "Could not create catalog-resource object (catalog)")
        }
    }

    // Delete the catalog.
    os.Remove(*catalogFilepath)

    l.Debug("Catalog pruned.", "catalogFilepath", *catalogFilepath)

    return relPath, nil
}

// First, assume that the path argument is a path. If a catalog doesn't exist 
// for it, then assume that we were given a filename, too, and see if we can 
// find a catalog after stripping that filename.
func findCatalog (catalogPath *string, relPath *string, hashAlgorithm *string) (*string, *string, error) {
    l := NewLogger("catalog")

    cc, err := newCatalogCommon(hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create catalog-common object (recall-hash)")
        return nil, nil, errorNew
    }

    catalogFilename, err := cc.getCatalogFilename(relPath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not formulate catalog filename", "recallScanRelPath", *relPath)
        return nil, nil, errorNew
    }

    catalogFilepath := path.Join(*catalogPath, *catalogFilename)

    f, err := os.Open(catalogFilepath)
    if err == nil {
        return &catalogFilepath, nil, nil
    }

    f.Close()

    // Now, assume that we might've been given a specific filename entry, as well.

    d := filepath.Dir(*relPath)
    strippedPath := &d
    if *strippedPath == "." {
        strippedPath = nil
    }

    b := filepath.Base(*relPath)
    entryFilename := &b

    catalogFilename, err = cc.getCatalogFilename(strippedPath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not formulate catalog filename from stripped path")
        return nil, nil, errorNew
    }

    catalogFilepath = path.Join(*catalogPath, *catalogFilename)

    f, err = os.Open(catalogFilepath)
    if err != nil {
        errorNew := l.LogError("Could not find catalog as a path *or* a file.")
        return nil, nil, errorNew
    }

    f.Close()

    return &catalogFilepath, entryFilename, nil
}

func RecallHash (catalogPath *string, recallScanRelPath *string, hashAlgorithm *string) (*string, error) {
    l := NewLogger("catalog")
    
    catalogFilepath, entryFilename, err := findCatalog(catalogPath, recallScanRelPath, hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not find catalog")
        return nil, errorNew
    }

    l.Debug("Recalling hash.", "catalogFilename", *catalogFilepath, "fileEntryGiven", entryFilename != nil)

    var hash *string

    readHash := func (cr *catalogResource) error {
        if entryFilename != nil {
            lr, err := cr.lookup(entryFilename)
            if err != nil {
                errorNew := l.MergeAndLogError(err, "Could not look in the matching catalog", "catalogFilepath", catalogFilepath)
                return errorNew
            }

            if lr.WasFound == false {
                errorNew := l.LogError("Could not find the given file in the matching catalog", "catalogFilepath", catalogFilepath, "filename", *entryFilename)
                return errorNew
            }

            hash = &lr.entry.hash
        } else {
            var err error

            hash, err = cr.getLastPathHash()
            if err != nil {
                errorNew := l.MergeAndLogError(err, "Could not get last path hash", "catalogFilepath", catalogFilepath)
                return errorNew
            }
        }

        return nil
    }

    err = executeWithCatalog(catalogFilepath, hashAlgorithm, readHash)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not read catalog or lookup path hash", "catalogFilepath", catalogFilepath, "recallScanRelPath", *recallScanRelPath)
        return nil, errorNew
    }

    return hash, nil
}
