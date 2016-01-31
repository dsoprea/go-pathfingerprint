package pfinternal

import (
    "fmt"
    "strings"
    "strconv"
    "path"
    "errors"

    "database/sql"
    "runtime/debug"

    _ "github.com/mattn/go-sqlite3"
)

type catalogResource struct {
    catalogFilepath *string
    db *sql.DB
    cc *catalogCommon
}

func NewCatalogResource(catalogFilepath *string, hashAlgorithm *string) (cr *catalogResource, err error) {
    l := NewLogger("catalog_resource")

    defer func() {
        if r := recover(); r != nil {
            cr = nil
            originalErr := r.(error)

            err = l.MergeAndLogError(originalErr, "Could not create catalog resource", "catalogFilepath", *catalogFilepath)
        }
    }()

    cc, err := newCatalogCommon(hashAlgorithm)
    if err != nil {
        panic(err)
    }

    cr = &catalogResource { 
            catalogFilepath: catalogFilepath,
            cc: cc,
    }

    return cr, nil
}

// TODO(dustin): We need to be able to tell Open() to not make any changes (in 
//               no-updates mode).

func (self *catalogResource) Open() (err error) {
    var db *sql.DB

    l := NewLogger("catalog_resource")

    defer func() {
        if r := recover(); r != nil {
            originalErr := r.(error)

            fmt.Printf("%s\n", debug.Stack())

            err = l.MergeAndLogError(originalErr, "Could not open catalog-resource")
        }
    }()

    l.Debug("Opening catalog resource.")

    if self.db != nil {
        panic(errors.New("Connection already opened."))
    }

    // Open the DB.

    db, err = sql.Open(DbType, *self.catalogFilepath)
    if err != nil {
        panic(err)
    }

    // Make sure the table exists.

    h, err := self.cc.getHashObject()
    if err != nil {
        panic(err)
    }

    query := 
        "CREATE TABLE `path_info` (" +
            "`path_info_id` INTEGER NOT NULL PRIMARY KEY, " +
            "`rel_path` VARCHAR(1000) NOT NULL, " +
            "`hash` VARCHAR(" + strconv.Itoa(h.Size() * 2) + ") NULL, " +
            "`schema_version` INTEGER NOT NULL DEFAULT 1, " +
            "`last_check_epoch` INTEGER UNSIGNED NULL DEFAULT 0, " +
            "CONSTRAINT `path_info_rel_path_idx` UNIQUE (`rel_path`)" +
        ")"

    err = self.createTable(db, "path_info", &query)
    if err != nil {
        panic(err)
    }

    err = self.createIndex(db, "path_info_last_check_epoch_idx", "path_info", "last_check_epoch", true)
    if err != nil {
        panic(err)
    }

    query = 
        "CREATE TABLE `catalog_entries` (" +
            "`catalog_entry_id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, " +
            "`path_info_id` INTEGER NOT NULL, " +
            "`filename` VARCHAR(255) NOT NULL, " +
            "`hash` VARCHAR(" + strconv.Itoa(h.Size() * 2) + ") NOT NULL, " +
            "`mtime_epoch` INTEGER UNSIGNED NOT NULL, " +
            "`last_check_epoch` INTEGER UNSIGNED NULL DEFAULT 0, " +
            "CONSTRAINT `catalog_entries_filename_idx` UNIQUE (`filename`, `path_info_id`), " +
            "CONSTRAINT `catalog_entries_path_info_id_fk` FOREIGN KEY (`path_info_id`) REFERENCES `path_info` (`path_info_id`)" +
        ")"

    err = self.createTable(db, "catalog_entries", &query)
    if err != nil {
        panic(err)
    }

    err = self.createIndex(db, "catalog_entries_last_check_epoch_idx", "catalog_entries", "last_check_epoch", true)
    if err != nil {
        panic(err)
    }

    self.db = db

    return nil
}

func (self *catalogResource) createTable(db *sql.DB, tableName string, tableQuery *string) error {
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

func (self *catalogResource) createIndex(db *sql.DB, indexName string, tableName string, columnName string, isAscending bool) error {
    l := NewLogger("catalog-resource")

    var suffixModifier string;
    if isAscending == true {
        suffixModifier = "ASC"
    } else {
        suffixModifier = "DESC"
    }

    query := fmt.Sprintf("CREATE INDEX %s ON `%s`(`%s` %s)", indexName, tableName, columnName, suffixModifier)

    _, err := db.Exec(query)
    if err != nil {
        // Check for something like this: table `catalog` already exists
        if strings.HasSuffix(err.Error(), "already exists") {
            l.Debug("Index already exists.", "TableName", tableName, "IndexName", indexName)
        } else {
            errorNew := l.MergeAndLogError(err, "Could not create index", "index", indexName)
            return errorNew
        }
    }

    return nil
}

func (self *catalogResource) Close() error {
    l := NewLogger("catalog-resource")

    l.Debug("Closing catalog resource.")

    if self.db == nil {
        errorNew := l.LogError("Connection not open and can't be closed.")
        return errorNew
    }

    self.db.Close()
    self.db = nil

    l.Debug("Catalog resource closed.")

    return nil
}

/*

// Update the catalog with the info for the path that the catalog represents. 
// This action allows us to determine when the directory is new or when the 
// contents have changed. 
func (self *catalogResource) setPathHash(relPath *string, hash *string) (ps int, err error) {
    l := NewLogger("catalog-resource")

// TODO(dustin): !! Refactor to just update.

    defer func() {
        if r := recover(); r != nil {
            ps = 0
            originalErr := r.(error)

            err = l.MergeAndLogError(originalErr, "Could not set path hash", "relPath", *relPath, "hash", *hash)
        }
    }()

    l.Debug("Updating path hash.", "relPath", *relPath, "hash", *hash)

    var currentHash string

    l.Debug("Updating existing path-info record.", "relPath", *relPath)

    err = rows.Scan(&currentHash)
    if err != nil {
        panic(err)
    }

    rows.Close()

    if currentHash == *hash {
        return PathStateUnaffected, nil
    }

    // The hash has changed.

// TODO(dustin): Can we use an alias on the table here?
    query := 
        "UPDATE " +
            "`path_info` " +
        "SET " +
            "`hash` = ? " +
        "WHERE " +
            "`rel_path` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    _, err = stmt.Exec(*hash, *relPath)
    if err != nil {
        panic(err)
    }

    l.Debug("Path-info has been updated.")
}
*/
func (self *catalogResource) lookupFile(pd *pathDescriptor, filename *string) (flr *fileLookupResult, err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            flr = nil
            originalErr := r.(error)

            fmt.Printf("%s\n", debug.Stack())

            err = l.MergeAndLogError(originalErr, "Could not do file entry lookup", "relPath", pd.GetRelPath(), "filename", *filename)
        }
    }()

    if pd.GetPathInfoId() == 0 {
        flr = newNotFoundFileLookupResult(pd, filename)
    } else {
        query := 
            "SELECT " +
                "`ce`.`catalog_entry_id`, " +
                "`ce`.`hash`, " +
                "`ce`.`mtime_epoch` " +
            "FROM " +
                "`catalog_entries` `ce` " +
            "WHERE " +
                "`ce`.`filename` = ? AND " +
                "`ce`.`path_info_id` = ?"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not prepare file-lookup query")
            return nil, errorNew
        }

        pathInfoId := pd.GetPathInfoId()

        rows, err := stmt.Query(*filename, pathInfoId)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not execute file-lookup")
            return nil, errorNew
        }

        defer rows.Close()

        if rows.Next() == false {
            // We don't yet know about this file.

            l.Debug("Filename not yet in catalog", "relPath", pd.GetRelPath(), "filename", *filename)

            flr = newNotFoundFileLookupResult(pd, filename)
        } else {
            // We already know about this file.

            l.Debug("Filename IS ALREADY in catalog", "relPath", pd.GetRelPath(), "filename", *filename)

            var catalogEntryId int
            var hash string
            var mtimeEpoch int64

            err = rows.Scan(&catalogEntryId, &hash, &mtimeEpoch)
            if err != nil {
                errorNew := l.MergeAndLogError(err, "Could not parse file-lookup result record")
                return nil, errorNew
            }

            ce := newCatalogEntry(catalogEntryId, &hash, mtimeEpoch)
            flr = newFoundFileLookupResult(pd, filename, ce)
        }
    }

    return flr, nil
}

func (self *catalogResource) lookupPath(relPath *string) (flr *pathLookupResult, err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            flr = nil
            originalErr := r.(error)

            fmt.Printf("relPath: [%s]\n", relPath)
            fmt.Printf("Stacktrace:\n%s\n", debug.Stack())

            err = l.MergeAndLogError(originalErr, "Could not do path-lookup", "relPath", *relPath)
        }
    }()

    query := 
        "SELECT " +
            "`pi`.`path_info_id`, " +
            "`pi`.`hash` " +
        "FROM " +
            "`path_info` `pi` " +
        "WHERE " +
            "`pi`.`rel_path` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare path-lookup query")
        return nil, errorNew
    }

    rows, err := stmt.Query(relPath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute path-lookup")
        return nil, errorNew
    }

    defer rows.Close()

    var plr *pathLookupResult

    if rows.Next() == false {
        // We don't yet know about this file.

        l.Debug("Path not yet in catalog", "relPath", *relPath)

        plr = newNotFoundPathLookupResult(relPath)
    } else {
        // We already know about this file.

        l.Debug("Path IS ALREADY in catalog", "relPath", *relPath)

        var pathInfoId int
        var hash string

        err = rows.Scan(&pathInfoId, &hash)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not parse path-lookup result record")
            return nil, errorNew
        }

        entry := newPathEntry(pathInfoId, &hash)
        plr = newFoundPathLookupResult(relPath, entry)
    }

    return plr, nil
}

func (self *catalogResource) updateLastFileCheck(flr *fileLookupResult, nowEpoch int64) error {
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

    r, err := stmt.Exec(nowEpoch, flr.entry.id)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute found-update query")
        return errorNew
    }

    affected, err := r.RowsAffected()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get the number of affected rows from the found-update query")
        return errorNew
    }

    l.Debug("Epoch updated.", "id", flr.entry.id, "affected", affected)

    if affected < 1 {
        errorNew := l.LogError("No rows were affected by the found-update query")
        return errorNew
    }

    return nil
}

func (self *catalogResource) updateLastPathCheck(plr *pathLookupResult, nowEpoch int64) error {
    l := NewLogger("catalog-resource")

// TODO(dustin): Can we use an alias on the table here?
    query := 
        "UPDATE " +
            "`path_info` " +
        "SET " +
            "`last_check_epoch` = ? " +
        "WHERE " +
            "`path_info_id` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare path found-update query")
        return errorNew
    }

    r, err := stmt.Exec(nowEpoch, plr.entry.id)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute path found-update query")
        return errorNew
    }

    affected, err := r.RowsAffected()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get the number of affected rows from the path found-update query")
        return errorNew
    }

    l.Debug("Epoch updated for path.", "id", plr.entry.id, "affected", affected)

    if affected < 1 {
        return l.LogError("No rows were affected by the path found-update query")
    } else if affected > 1 {
        return l.LogError("Too many rows were affected by the path found-update query")
    }

    return nil
}

func (self *catalogResource) setFile(flr *fileLookupResult, mtime int64, hash *string, nowEpoch int64) error {
    l := NewLogger("catalog-resource")

    if flr.wasFound == true {
        l.Debug("Updating entry", "filename", flr.filename, "id", flr.entry.id, "mtime", mtime, "hash", *hash)

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

        r, err := stmt.Exec(*hash, mtime, flr.entry.id)
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

        // Just an assertion. This should never be the case.
        if(flr.pd.GetPathInfoId() == 0) {
            panic(errors.New("Can't insert a file without a valid path ID."))
        }

        l.Debug("Inserting entry", "filename", flr.filename, "mtime", mtime, "hash", *hash, "last_check_epoch", nowEpoch)

        query := 
            "INSERT INTO `catalog_entries` " +
                "(`path_info_id`, `filename`, `hash`, `mtime_epoch`, `last_check_epoch`) " +
            "VALUES " +
                "(?, ?, ?, ?, ?)"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not prepare entry-insert query")
            return errorNew
        }

        _, err = stmt.Exec(flr.pd.GetPathInfoId(), flr.filename, *hash, mtime, nowEpoch)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not execute entry-insert query")
            return errorNew
        }
    }

    return nil
}

func (self *catalogResource) createPath(relPath *string, nowEpoch int64) (id int, err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            id = 0
            originalErr := r.(error)

            fmt.Printf("Stack:\n%s\n", debug.Stack())

            err = l.MergeAndLogError(originalErr, "Could not create path", "relPath", *relPath)
        }
    }()

    l.Debug("Inserting path-info record.", "relPath", *relPath, "nowEpoch", nowEpoch)

    query := 
        "INSERT INTO `path_info` " +
            "(`rel_path`, `last_check_epoch`) " +
        "VALUES " +
            "(?, ?)"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    res, err := stmt.Exec(*relPath, nowEpoch)
    if err != nil {
        panic(err)
    }

    idInt64, err := res.LastInsertId()
    if err != nil {
        panic(err)
    }

    l.Debug("Path record inserted.", "id", idInt64)

    return int(idInt64), nil
}

func (self *catalogResource) updatePath(pd *pathDescriptor, hash *string) error {
    l := NewLogger("catalog-resource")

    l.Debug("Updating path", "relPath", pd.GetRelPath(), "id", pd.GetPathInfoId(), "hash", *hash)

// TODO(dustin): Can we use an alias on the table here?
    query := 
        "UPDATE " +
            "`path_info` " +
        "SET " +
            "`hash` = ? " +
        "WHERE " +
            "`path_info_id` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare path-update query")
        return errorNew
    }

    r, err := stmt.Exec(*hash, pd.GetPathInfoId())
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute path-update query")
        return errorNew
    }

    affected, err := r.RowsAffected()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get the number of affected rows from the path-update query")
        return errorNew
    }

    if affected < 1 {
        return l.LogError("No rows were affected by the path-update query")
    } else if affected > 1 {
        return l.LogError("Too many rows were affected by the path-update query")
    }

    return nil
}

// Get a list of all file records that haven't been touched in this run 
// (because all of the ones that match known files have been updated to a later 
// timestamp than they had).
func (self *catalogResource) pushOldFileEntries(nowEpoch int64, c chan<- *ChangeEvent) error {
    l := NewLogger("catalog-resource")

    // If we're reporting changes, then enumerate the entries to be delete 
    // and push them up.

    query := 
        "SELECT " +
            "`pi`.`rel_path`, " +
            "`ce`.`filename` " +
        "FROM " +
            "`catalog_entries` `ce`, " +
            "`path_info` `pi` " +
        "WHERE " +
            "`ce`.`last_check_epoch` < ? AND " +
            "`pi`.`path_info_id` = `ce`.`path_info_id`"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare old-files query")
        return errorNew
    }

    rows, err := stmt.Query(nowEpoch)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute old-files query")
        return errorNew
    }

    defer rows.Close()

    for rows.Next() {
        var relPath string
        var filename string

        err = rows.Scan(&relPath, &filename)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not parse filename from old-files query")
            return errorNew
        }

        relFilepath := path.Join(relPath, filename)

        c <- &ChangeEvent { 
                EntityType: EntityTypeFile, 
                ChangeType: UpdateTypeDelete, 
                RelPath: relFilepath,
        }
    }

    return nil
}

// Get a list of all path records that haven't been touched in this run 
// (because all of the ones that match known files have been updated to a later 
// timestamp than they had).
func (self *catalogResource) pushOldPathEntries(nowEpoch int64, c chan<- *ChangeEvent) error {
    l := NewLogger("catalog-resource")

    // If we're reporting changes, then enumerate the entries to be delete 
    // and push them up.

    query := 
        "SELECT " +
            "`pi`.`rel_path` " +
        "FROM " +
            "`path_info` `pi` " +
        "WHERE " +
            "`pi`.`last_check_epoch` < ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare old-paths query")
        return errorNew
    }

    rows, err := stmt.Query(nowEpoch)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute old-paths query")
        return errorNew
    }

    defer rows.Close()

    for rows.Next() {
        var relPath string

        err = rows.Scan(&relPath)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not parse rel-path from old-paths query")
            return errorNew
        }

        c <- &ChangeEvent { 
                EntityType: EntityTypePath, 
                ChangeType: UpdateTypeDelete, 
                RelPath: relPath,
        }
    }

    return nil
}

// Delete all records that haven't been touched in this run (because all of the 
// ones that match known files have been updated to a later timestamp than they 
// had).
func (self *catalogResource) pruneOldFiles(nowEpoch int64, c chan<- *ChangeEvent) error {
    l := NewLogger("catalog-resource")

    if c != nil {
        err := self.pushOldFileEntries(nowEpoch, c)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not report old FILE entries")
            return errorNew
        }
    }

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

    l.Debug("Pruned old FILE entries.", "affected", affected)

    return nil
}

func (self *catalogResource) pruneOldPaths(nowEpoch int64, c chan<- *ChangeEvent) error {
    l := NewLogger("catalog-resource")

    if c != nil {
        err := self.pushOldPathEntries(nowEpoch, c)
        if err != nil {
            errorNew := l.MergeAndLogError(err, "Could not report old PATH entries")
            return errorNew
        }
    }

    query := 
        "DELETE FROM `path_info` " +
        "WHERE " +
            "`last_check_epoch` < ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare paths-prune query")
        return errorNew
    }

    r, err := stmt.Exec(nowEpoch)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute paths-prune query")
        return errorNew
    }

    affected, err := r.RowsAffected()
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not get the number of affected rows from the paths-prune query")
        return errorNew
    }

    l.Debug("Pruned old PATH entries.", "affected", affected)

    return nil
}

func (self *catalogResource) getLastPathHash(relPath *string) (*string, error) {
    var hash string

    l := NewLogger("catalog-resource")

    query := 
        "SELECT " +
            "`pi`.`hash` " +
        "FROM " +
            "`path_info` `pi` " +
        "WHERE " +
            "`pi`.`rel_path` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not prepare path-info identification query (getLastPathHash)")
        return nil, errorNew
    }

    rows, err := stmt.Query(*relPath)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not execute path-info identification query (getLastPathHash)")
        return nil, errorNew
    }

    defer rows.Close()

    if rows.Next() == false {
        errorNew := l.LogError("Path-info identification result was erroneously empty (getLastPathHash).")
        return nil, errorNew
    }

    err = rows.Scan(&hash)
    if err != nil {
        errorNew := l.MergeAndLogError(err, "Could not parse path-info identification result record (getLastPathHash)")
        return nil, errorNew
    }

    return &hash, nil
}
