package pfinternal

import (
    "os"
    "fmt"
    "path"
    "errors"
    "time"
    "strings"

    "crypto/sha1"
    "database/sql"

    _ "github.com/mattn/go-sqlite3"
)

const (
    PathCreationMode = 0755
    DbType = "sqlite3"
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
    nowEpoch int64
}

func NewCatalog (catalogPath *string, scanPath *string, allowUpdates bool) (*Catalog, error) {
    l := NewLogger()

    err := os.MkdirAll(*catalogPath, PathCreationMode)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not create catalog path", "catalogPath", *catalogPath)
        return nil, errorNew
    }

    catalogFilename := fmt.Sprintf("%x", sha1.Sum([]byte(*scanPath)))
    catalogFilepath := path.Join(*catalogPath, catalogFilename)
    nowEpoch := time.Now().Unix()

    c := Catalog { 
            catalogPath: catalogPath, 
            scanPath: scanPath, 
            allowUpdates: allowUpdates,
            relScanPath: nil, 
            catalogFilename: &catalogFilename, 
            catalogFilepath: &catalogFilepath,
            nowEpoch: nowEpoch }

    return &c, nil
}

func (self *Catalog) GetCatalogPath () *string {
    return self.catalogPath
}

func (self *Catalog) GetCatalogFilepath () *string {
    return self.catalogFilepath
}

func (self *Catalog) BranchCatalog (childPathName *string) (*Catalog, error) {
    scanPath := path.Join(*self.scanPath, *childPathName)

    var relScanPath string
    if self.relScanPath == nil {
        relScanPath = *childPathName
    } else {
        relScanPath = path.Join(*self.relScanPath, *childPathName)
    }

    catalogFilename := fmt.Sprintf("%x", sha1.Sum([]byte(scanPath)))
    catalogFilepath := path.Join(*self.catalogPath, catalogFilename)

    c := Catalog{ 
            catalogPath: self.catalogPath, 
            scanPath: &scanPath, 
            allowUpdates: self.allowUpdates,
            relScanPath: &relScanPath, 
            catalogFilename: &catalogFilename, 
            catalogFilepath: &catalogFilepath,
            nowEpoch: self.nowEpoch }

    return &c, nil
}

func (self *Catalog) Open () error {
    var query string
    var err error
    var db *sql.DB

    l := NewLogger()

    l.Debug("Opening database.", "catalogFilepath", *self.catalogFilepath)

    if self.db != nil {
        return errors.New("Connection already opened.")        
    }

    // Open the DB.

    db, err = sql.Open(DbType, *self.catalogFilepath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not connect DB", "DbType", DbType, "DbFilename", self.catalogFilepath)
        return errorNew
    }
/*
    // Lock the file.

    query = "BEGIN EXCLUSIVE TRANSACTION"

    _, err = db.Exec(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not open transaction")
        return errorNew
    }
*/
    // Make sure the table exists.

    query = 
        "CREATE TABLE `catalog_entries` (" +
            "`catalog_entry_id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, " +
            "`filename` VARCHAR(255) NOT NULL, " +
            "`sha1` VARCHAR(40) NOT NULL, " +
            "`mtime_epoch` INTEGER UNSIGNED NOT NULL, " +
            "`last_check_epoch` INTEGER UNSIGNED NULL DEFAULT 0, " +
            "CONSTRAINT `filename_idx` UNIQUE (`filename`)" +
        ")"

    _, err = db.Exec(query)

    if err != nil {
        // Check for something like this: table `catalog` already exists
        if strings.HasSuffix(err.Error(), "already exists") {
            l.Debug("Catalog table already exists.", "catalogFilepath", *self.catalogFilepath)
        } else {
            errorNew := l.MergeAndLogError(err, "Could not create table")
            return errorNew
        }
    }

    self.db = db

    return nil
}

func (self *Catalog) Close () error {
//    var query string
//    var err error

    l := NewLogger()

    l.Debug("Closing database.", "catalogFilepath", *self.catalogFilepath)

    if self.db == nil {
        return errors.New("Connection not open and can't be closed.")
    }
/*
    query = "COMMIT TRANSACTION"

    _, err = self.db.Exec(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not commit transaction")
        return errorNew
    }
*/
    self.db = nil

    return nil
}

func (self *Catalog) Lookup (filename *string) (*LookupResult, error) {
    var query string
    var err error
    var lr LookupResult

    l := NewLogger()

    query = 
        "SELECT " +
            "`ce`.`catalog_entry_id`, " +
            "`ce`.`sha1`, " +
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

    if rows.Next() == false {
        // We don't yet know about this file.

        l.Debug("Filename not yet in catalog", "filename", *filename)

        lr.WasFound = false
        lr.filename = filename
    } else {
        // We already know about this file.

        l.Debug("Filename IS ALREADY in catalog", "filename", *filename)

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

// TODO(dustin): Finish debugging the alias syntax.
/*
            query = 
                "UPDATE " +
                    "`catalog_entries` `ce` " +
                "SET " +
                    "`ce`.`last_check_epoch` = ? " +
                "WHERE " +
                    "`ce`.`catalog_entry_id` = ?"
*/
            query = 
                "UPDATE " +
                    "catalog_entries " +
                "SET " +
                    "last_check_epoch = ? " +
                "WHERE " +
                    "catalog_entry_id = ?"

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
            } else if affected < 1 {
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

func (self *Catalog) Update (lr *LookupResult, mtime int64, sha1 *string) error {
    var query string

    l := NewLogger()

    if self.allowUpdates == false {
        // We were told to not make any changes.

        return nil
    } else if lr.WasFound == true && lr.entry.mtime == mtime {
        // The entry already existed and the mtime matched.

        return nil
    }

    if lr.WasFound == true {
        l.Debug("Updating entry", "filename", *lr.filename, "id", lr.entry.id, "mtime", lr.entry.mtime, "sha1", lr.entry.hash)

// TODO(dustin): Finish debugging the alias syntax.
/*
        query = 
            "UPDATE " +
                "`catalog_entries` AS `ce` " +
            "SET " +
                "`ce`.`sha1` = ?, " +
                "`ce`.`mtime_epoch` = ? " +
            "WHERE " +
                "`ce`.`catalog_entry_id` = ?"
*/
        query = 
            "UPDATE " +
                "catalog_entries " +
            "SET " +
                "sha1 = ?, " +
                "mtime_epoch = ? " +
            "WHERE " +
                "catalog_entry_id = ?"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not prepare entry-update query")
            return errorNew
        }

        r, err := stmt.Exec(sha1, mtime, lr.entry.id)
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

        l.Debug("Inserting entry", "filename", *lr.filename, "mtime", mtime, "sha1", *sha1, "last_check_epoch", self.nowEpoch)

        query = 
            "INSERT INTO `catalog_entries` " +
                "(`filename`, `sha1`, `mtime_epoch`, `last_check_epoch`) " +
            "VALUES " +
                "(?, ?, ?, ?)"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not prepare entry-insert query")
            return errorNew
        }

        _, err = stmt.Exec(lr.filename, sha1, mtime, self.nowEpoch)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not execute entry-insert query")
            return errorNew
        }
    }

    return nil
}

// Delete all records that haven't been touched in this run.
func (self *Catalog) PruneOld () error {
    var query string

    l := NewLogger()

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
