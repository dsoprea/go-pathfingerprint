package pfinternal

import (
    "os"
    "fmt"
    "path"
    "io"
    "errors"
    "time"
    "strings"
    "hash"
    "strconv"
//    "reflect"

    "database/sql"

    _ "github.com/mattn/go-sqlite3"
)

const (
    PathCreationMode = 0755
    DbType = "sqlite3"
    CatalogPathListBatchSize = 3
    RootCatalogFilename = "root"
)

var ErrNoHash = errors.New("no hash recorded for the filename")
var ErrFileChanged = errors.New("mtime for filename does not match")

type catalogEntry struct {
    id int
    hash string
    mtime int64
}

type LookupResult struct {
    WasFound bool
    filename *string
    entry *catalogEntry
}

type Catalog struct {
    catalogPath *string
    scanPath *string
    allowUpdates bool
    relScanPath *string
    catalogFilename *string
    catalogFilepath *string
    db *sql.DB
    nowTime time.Time
    nowEpoch int64
    hashAlgorithm *string
    reportingChannel chan<- *ChangeEvent
}

func NewCatalog (catalogPath *string, scanPath *string, allowUpdates bool, hashAlgorithm *string, reportingChannel chan<- *ChangeEvent) (*Catalog, error) {
    l := NewLogger("catalog")

    err := os.MkdirAll(*catalogPath, PathCreationMode)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create catalog path", "catalogPath", *catalogPath)
        return nil, errorNew
    }

    catalogFilename := RootCatalogFilename
    catalogFilepath := path.Join(*catalogPath, catalogFilename)

    nowTime := time.Now()
    nowEpoch := nowTime.Unix()

    l.Debug("Current time.", "nowEpoch", nowEpoch)

    c := Catalog { 
            catalogPath: catalogPath, 
            scanPath: scanPath, 
            allowUpdates: allowUpdates,
            relScanPath: nil, 
            catalogFilename: &catalogFilename, 
            catalogFilepath: &catalogFilepath,
            nowTime: nowTime,
            nowEpoch: nowEpoch,
            hashAlgorithm: hashAlgorithm,
            reportingChannel: reportingChannel,
    }

    return &c, nil
}

func (self *Catalog) GetCatalogPath () *string {
    return self.catalogPath
}

func (self *Catalog) GetCatalogFilepath () *string {
    return self.catalogFilepath
}

func (self *Catalog) getHashObject () (hash.Hash, error) {
    l := NewLogger("catalog")
    
    h, err := getHashObject(self.hashAlgorithm)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (catalog)", "hashAlgorithm", *self.hashAlgorithm)
        return nil, errorNew
    }

    return h, nil
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

    h, err := self.getHashObject()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (branch-catalog)")
        return nil, errorNew
    }

    h.Write([]byte(relScanPath))

    catalogFilename := fmt.Sprintf("%x", h.Sum(nil))
    catalogFilepath := path.Join(*self.catalogPath, catalogFilename)

    c := Catalog { 
            catalogPath: self.catalogPath, 
            scanPath: &scanPath, 
            allowUpdates: self.allowUpdates,
            relScanPath: &relScanPath, 
            catalogFilename: &catalogFilename, 
            catalogFilepath: &catalogFilepath,
            nowTime: self.nowTime,
            nowEpoch: self.nowEpoch,
            hashAlgorithm: self.hashAlgorithm,
            reportingChannel: self.reportingChannel }

    return &c, nil
}

func (self *Catalog) Open () error {
    var query string
    var err error
    var db *sql.DB

    l := NewLogger("catalog")

    l.Debug("Opening catalog.", "catalogFilepath", *self.catalogFilepath)

    if self.db != nil {
        return errors.New("Connection already opened.")        
    }

    // Open the DB.

    db, err = sql.Open(DbType, *self.catalogFilepath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not connect DB", "DbType", DbType, "DbFilename", self.catalogFilepath)
        return errorNew
    }

    // Make sure the table exists.

    h, err := self.getHashObject()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get hash object (catalog-open)")
        return errorNew
    }

    query = 
        "CREATE TABLE `path_info` (" +
            "`path_info_id` INTEGER NOT NULL PRIMARY KEY, " +
            "`rel_path` VARCHAR(1000) NOT NULL, " +
            "`hash` VARCHAR(" + strconv.Itoa(h.Size() * 2) + ") NOT NULL " +
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

func (self *Catalog) createTable (db *sql.DB, tableName string, tableQuery *string) error {
    l := NewLogger("catalog")

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

func (self *Catalog) Close () error {
    l := NewLogger("catalog")

    l.Debug("Closing catalog.", "catalogFilepath", *self.catalogFilepath)

    if self.db == nil {
        return errors.New("Connection not open and can't be closed.")
    }

    self.db.Close()
    self.db = nil

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
    var query string

    l := NewLogger("catalog")

    if self.reportingChannel != nil {
        l.Debug("Reading catalog-to-be-pruned.", "catalogFilepath", *catalogFilepath)

        // Open the catalog in order to determine what path it represents.

        db, err := sql.Open(DbType, *catalogFilepath)
        if err != nil {
            l.MergeAndLogError(err, "Could not connect to DB for prune", "catalogFilepath", *catalogFilepath)
        } else {
            query = 
                "SELECT " +
                    "`pi`.`rel_path` " +
                "FROM " +
                    "`path_info` `pi` " +
                "WHERE " +
                    "`pi`.`path_info_id` = 1"

            rows, err := db.Query(query)
            if err != nil {
                l.MergeAndLogError(err, "Could not prepare path-info identification query (pruning)")
            } else {
                if rows.Next() == false {
                    l.Error("Path-info identification result was erroneously empty (pruning).", "catalogFilepath", *catalogFilepath)
                } else {
                    var relPath string

                    err = rows.Scan(&relPath)
                    if err != nil {
                        l.MergeAndLogError(err, "Could not parse path-info identification result record (pruning)")
                    } else {
                        self.reportingChannel <- &ChangeEvent { EntityType: &EntityTypePath, ChangeType: &UpdateTypeDelete, RelPath: &relPath }
                    }
                }

                rows.Close()
            }

            db.Close()
        }
    }

    // Delete the catalog.

    os.Remove(*catalogFilepath)

    // Emit an event.

    l.Debug("Catalog pruned.", "catalogFilepath", *catalogFilepath)

    return nil
}

// Update the catalog with the info for the path that the catalog represents. 
// This action allows us to determine when the directory is new or when the 
// contents have changed. 
func (self *Catalog) SetPathHash (relPath *string, hash *string) (*string, error) {
    var query string

    l := NewLogger("catalog")

    l.Debug("Updating path hash.", "catalogFilepath", *self.catalogFilepath, "relPath", *relPath, "hash", *hash)

    query = 
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
        query = 
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

        query = 
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

func (self *Catalog) Lookup (filename *string) (*LookupResult, error) {
    var query string
    var err error
    var lr LookupResult

    l := NewLogger("catalog")

    query = 
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

        rows.Close()

        if self.allowUpdates == true {
            // Update the timestamp of the record so that we can determine 
            // which records no longer represent valid files.

            l.Debug("Setting last_check_epoch for entry", "filename", *filename, "last_check_epoch", self.nowEpoch)

// TODO(dustin): Can we use an alias on the table here?
            query = 
                "UPDATE " +
                    "`catalog_entries` " +
                "SET " +
                    "`last_check_epoch` = ? " +
                "WHERE " +
                    "`catalog_entry_id` = ?"

            stmt, err := self.db.Prepare(query)
            if err != nil {
                errorNew := l.MergeAndLogError(err, "Could not prepare found-update query")
                return nil, errorNew
            }

            r, err := stmt.Exec(self.nowEpoch, entry.id)
            if err != nil {
                errorNew := l.MergeAndLogError(err, "Could not execute found-update query")
                return nil, errorNew
            }

            affected, err := r.RowsAffected()
            if err != nil {
                errorNew := l.MergeAndLogError(err, "Could not get the number of affected rows from the found-update query")
                return nil, errorNew
            }

            l.Debug("Epoch updated.", "id", entry.id, "affected", affected)

            if affected < 1 {
                errorNew := l.LogError("No rows were affected by the found-update query")
                return nil, errorNew
            }
        }

        lr.WasFound = true
        lr.filename = filename
        lr.entry = &entry
    }

    return &lr, nil
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

func (self *Catalog) Update (lr *LookupResult, mtime int64, hash *string) error {
    var query string

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

    if lr.WasFound == true {
        l.Debug("Updating entry", "filename", *lr.filename, "id", lr.entry.id, "mtime", lr.entry.mtime, "hash", lr.entry.hash)

// TODO(dustin): Can we use an alias on the table here?
        query = 
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

        l.Debug("Inserting entry", "filename", *lr.filename, "mtime", mtime, "hash", *hash, "last_check_epoch", self.nowEpoch)

        query = 
            "INSERT INTO `catalog_entries` " +
                "(`filename`, `hash`, `mtime_epoch`, `last_check_epoch`) " +
            "VALUES " +
                "(?, ?, ?, ?)"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not prepare entry-insert query")
            return errorNew
        }

        _, err = stmt.Exec(lr.filename, hash, mtime, self.nowEpoch)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not execute entry-insert query")
            return errorNew
        }
    }

    return nil
}

// Delete all records that haven't been touched in this run (because all of the 
// ones that match known files have been updated to a later timestamp than they 
// had).
func (self *Catalog) PruneOld () error {
    var query string

    l := NewLogger("catalog")

    if self.allowUpdates == false {
        l.Debug("Not checking for deletions since we're not allowed to make updates.")
        return nil
    }

    if self.reportingChannel != nil {
        // If we're reporting changes, then enumerate the entries to be delete 
        // and push them up.

        query = 
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

        rows, err := stmt.Query(self.nowEpoch)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not execute pruned-entries query")
            return errorNew
        }

        for rows.Next() {
            var filename string

            err = rows.Scan(&filename)
            if err != nil {
                rows.Close()

                errorNew := l.MergeAndLogError(err, "Could not parse filename from pruned-entries query")
                return errorNew
            }

            relFilepath := self.getFilePath(&filename)

            if self.reportingChannel != nil {
                self.reportingChannel <- &ChangeEvent { EntityType: &EntityTypeFile, ChangeType: &UpdateTypeDelete, RelPath: &relFilepath }
            }
        }

        rows.Close()
    }

    if self.allowUpdates == false {
        return nil
    }

    query = 
        "DELETE FROM `catalog_entries` " +
        "WHERE " +
            "`last_check_epoch` < ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare entries-prune query")
        return errorNew
    }

    r, err := stmt.Exec(self.nowEpoch)
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
