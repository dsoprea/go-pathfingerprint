package pfinternal

import (
    "fmt"
    "strings"
    "errors"
    "strconv"
    "path"

    "database/sql"

    _ "github.com/mattn/go-sqlite3"
)

const (
    CurrentSchemaVersion = 2
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
            err = r.(error)
            l.Error("Could not create catalog resource", 
                "catalogFilepath", *catalogFilepath, 
                "err", err)
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
    l := NewLogger("catalog_resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not open catalog resource", "err", err)
        }
    }()

    var db *sql.DB

    l.Debug("Opening catalog resource.")

    if self.db != nil {
        panic(errors.New("Connection already opened."))
    }

    // Open the DB.

    db, err = sql.Open(DbType, *self.catalogFilepath)
    if err != nil {
        panic(err)
    }

    query := 
        "CREATE TABLE `catalog_info` (\n" +
            "`catalog_info_id` INTEGER NOT NULL PRIMARY KEY, \n" +
            "`key` VARCHAR(50) NOT NULL UNIQUE, \n" +
            "`value` VARCHAR(200) NULL \n" +
        ")\n"

    wasCreated, err := self.createTable(db, "catalog_info", &query)
    if err != nil {
        panic(err)
    }

    if wasCreated == true {
        query := 
            "INSERT INTO `catalog_info` " +
                "(`key`, `value`) " +
            "VALUES " +
                "('schema_version', ?)"

        _, err = self.executeInsert(db, &query, CurrentSchemaVersion)
        if err != nil {
            panic(err)
        }
    }

    // Make sure the table exists.

    h, err := self.cc.getHashObject()
    if err != nil {
        panic(err)
    }

    query = 
        "CREATE TABLE `paths` (\n" +
            "`path_id` INTEGER NOT NULL PRIMARY KEY, \n" +
            "`rel_path` VARCHAR(1000) NOT NULL, \n" +
            "`hash` VARCHAR(" + strconv.Itoa(h.Size() * 2) + ") NULL, \n" +
            "`last_check_epoch` INTEGER UNSIGNED NULL DEFAULT 0, \n" +
            "CONSTRAINT `paths_rel_path_idx` UNIQUE (`rel_path`)\n" +
        ")\n"

    wasCreated, err = self.createTable(db, "paths", &query)
    if err != nil {
        panic(err)
    }

    if wasCreated == true {
        err = self.createIndex(db, "paths_last_check_epoch_idx", "paths", "last_check_epoch", true)
        if err != nil {
            panic(err)
        }
    }

    query = 
        "CREATE TABLE `files` (\n" +
            "`file_id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, \n" +
            "`path_id` INTEGER NOT NULL, \n" +
            "`filename` VARCHAR(255) NOT NULL, \n" +
            "`hash` VARCHAR(" + strconv.Itoa(h.Size() * 2) + ") NOT NULL, \n" +
            "`mtime_epoch` INTEGER UNSIGNED NOT NULL, \n" +
            "`last_check_epoch` INTEGER UNSIGNED NULL DEFAULT 0, \n" +
            "CONSTRAINT `files_filename_idx` UNIQUE (`filename`, `path_id`), \n" +
            "CONSTRAINT `files_path_id_fk` FOREIGN KEY (`path_id`) REFERENCES `paths` (`path_id`)\n" +
        ")\n"

    wasCreated, err = self.createTable(db, "files", &query)
    if err != nil {
        panic(err)
    }

    if wasCreated == true {
        err = self.createIndex(db, "files_last_check_epoch_idx", "files", "last_check_epoch", true)
        if err != nil {
            panic(err)
        }
    }

    self.db = db

    return nil
}

func (self *catalogResource) createTable(db *sql.DB, tableName string, tableQuery *string) (wasCreated bool, err error) {
    l := NewLogger("catalog-resource")

    l.Debug("Attempting to create table.", "name", tableName)

    wasCreated = false

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not create table", "err", err)
        }
    }()

    _, err = db.Exec(*tableQuery)
    if err != nil {
        // Check for something like this: table `catalog` already exists
        if strings.HasSuffix(err.Error(), "already exists") {
            l.Debug("Table already exists.", "Name", tableName)
        } else {
            panic(err)
        }
    } else {
        wasCreated = true
    }

    return wasCreated, nil
}

func (self *catalogResource) createIndex(db *sql.DB, indexName string, tableName string, columnName string, isAscending bool) (err error) {
    l := NewLogger("catalog-resource")

    l.Debug("Attempting to create index.", "name", indexName, "tableName", tableName)

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not create index.", "err", err)
        }
    }()

    var suffixModifier string;
    if isAscending == true {
        suffixModifier = "ASC"
    } else {
        suffixModifier = "DESC"
    }

    query := fmt.Sprintf("CREATE INDEX %s ON `%s`(`%s` %s)", indexName, tableName, columnName, suffixModifier)

    _, err = db.Exec(query)
    if err != nil {
        // Check for something like this: table `catalog` already exists
        if strings.HasSuffix(err.Error(), "already exists") {
            l.Debug("Index already exists.", 
                "TableName", tableName, 
                "IndexName", indexName)
        } else {
            panic(err)
        }
    }

    return nil
}

func (self *catalogResource) executeInsert(db *sql.DB, query *string, args ...interface{}) (id int64, err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            id = 0
            err = r.(error)

            l.Error("Could not insert record", "err", err)
        }
    }()

    stmt, err := db.Prepare(*query)
    if err != nil {
        panic(err)
    }

    res, err := stmt.Exec(args...)
    if err != nil {
        panic(err)
    }

    id, err = res.LastInsertId()
    if err != nil {
        panic(err)
    }

    return id, nil
}

func (self *catalogResource) Close() (err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not close catalog resource", "err", err)
        }
    }()

    l.Debug("Closing catalog resource.")

    if self.db == nil {
        panic(errors.New("Connection not open and can't be closed."))
    }

    self.db.Close()
    self.db = nil

    l.Debug("Catalog resource closed.")

    return nil
}

func (self *catalogResource) lookupFile(pd *pathDescriptor, filename *string) (flr *fileLookupResult, err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            flr = nil
            err = r.(error)

            l.Error("Could not open catalog resource", "err", err)
        }
    }()

    if pd.GetPathInfoId() == 0 {
        flr = newNotFoundFileLookupResult(pd, filename)
    } else {
        query := 
            "SELECT " +
                "`f`.`file_id`, " +
                "`f`.`hash`, " +
                "`f`.`mtime_epoch` " +
            "FROM " +
                "`files` `f` " +
            "WHERE " +
                "`f`.`filename` = ? AND " +
                "`f`.`path_id` = ?"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            panic(err)
        }

        pathInfoId := pd.GetPathInfoId()

        rows, err := stmt.Query(*filename, pathInfoId)
        if err != nil {
            panic(err)
        }

        defer rows.Close()

        if rows.Next() == false {
            // We don't yet know about this file.

            l.Debug("Filename not yet in catalog", 
                "relPath", pd.GetRelPath(), 
                "filename", *filename)

            flr = newNotFoundFileLookupResult(pd, filename)
        } else {
            // We already know about this file.

            l.Debug("Filename IS ALREADY in catalog", 
                "relPath", pd.GetRelPath(), 
                "filename", *filename)

            var catalogEntryId int
            var hash string
            var mtimeEpoch int64

            err = rows.Scan(&catalogEntryId, &hash, &mtimeEpoch)
            if err != nil {
                panic(err)
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
            err = r.(error)

            l.Error("Could not lookup path", "err", err)
        }
    }()

    query := 
        "SELECT " +
            "`p`.`path_id`, " +
            "`p`.`hash` " +
        "FROM " +
            "`paths` `p` " +
        "WHERE " +
            "`p`.`rel_path` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    rows, err := stmt.Query(relPath)
    if err != nil {
        panic(err)
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
            panic(err)
        }

        entry := newPathEntry(pathInfoId, &hash)
        plr = newFoundPathLookupResult(relPath, entry)
    }

    return plr, nil
}

func (self *catalogResource) updateLastFileCheck(flr *fileLookupResult, nowEpoch int64) (err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not update last file-check timestamp", "err", err)
        }
    }()

// TODO(dustin): Can we use an alias on the table here?
    query := 
        "UPDATE " +
            "`files` " +
        "SET " +
            "`last_check_epoch` = ? " +
        "WHERE " +
            "`file_id` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    r, err := stmt.Exec(nowEpoch, flr.entry.id)
    if err != nil {
        panic(err)
    }

    affected, err := r.RowsAffected()
    if err != nil {
        panic(err)
    }

    l.Debug("Epoch updated.", 
        "id", flr.entry.id, 
        "affected", affected)

    if affected < 1 {
        panic(err)
    }

    return nil
}

func (self *catalogResource) updateLastPathCheck(plr *pathLookupResult, nowEpoch int64) (err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not update last path-check", "err", err)
        }
    }()

// TODO(dustin): Can we use an alias on the table here?
    query := 
        "UPDATE " +
            "`paths` " +
        "SET " +
            "`last_check_epoch` = ? " +
        "WHERE " +
            "`path_id` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    r, err := stmt.Exec(nowEpoch, plr.entry.id)
    if err != nil {
        panic(err)
    }

    affected, err := r.RowsAffected()
    if err != nil {
        panic(err)
    }

    l.Debug("Epoch updated for path.", 
        "id", plr.entry.id, 
        "affected", affected)

    if affected < 1 {
        panic(errors.New("No rows were affected by the path found-update query"))
    } else if affected > 1 {
        panic(errors.New("Too many rows were affected by the path found-update query"))
    }

    return nil
}

func (self *catalogResource) setFile(flr *fileLookupResult, mtime int64, hash *string, nowEpoch int64) (err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not set file", "err", err)
        }
    }()

    if flr.wasFound == true {
        l.Debug("Updating entry", 
            "filename", flr.filename, 
            "id", flr.entry.id, 
            "mtime", mtime, 
            "hash", *hash)

// TODO(dustin): Can we use an alias on the table here?
        query := 
            "UPDATE " +
                "`files` " +
            "SET " +
                "`hash` = ?, " +
                "`mtime_epoch` = ? " +
            "WHERE " +
                "`file_id` = ?"

        stmt, err := self.db.Prepare(query)
        if err != nil {
            panic(err)
        }

        r, err := stmt.Exec(*hash, mtime, flr.entry.id)
        if err != nil {
            panic(err)
        }

        affected, err := r.RowsAffected()
        if err != nil {
            panic(err)
        }

        if affected < 1 {
            panic(errors.New("No rows were affected by the entry-update query"))
        } else if affected > 1 {
            panic(errors.New("Too many rows were affected by the entry-update query"))
        }
    } else {
        // The filename wasn't in the catalog. Add it.

        // Just an assertion. This should never be the case.
        if(flr.pd.GetPathInfoId() == 0) {
            panic(errors.New("Can't insert a file without a valid path ID."))
        }

        l.Debug("Inserting entry", 
            "filename", flr.filename, 
            "mtime", mtime, 
            "hash", *hash, 
            "last_check_epoch", nowEpoch)

        query := 
            "INSERT INTO `files` " +
                "(`path_id`, `filename`, `hash`, `mtime_epoch`, `last_check_epoch`) " +
            "VALUES " +
                "(?, ?, ?, ?, ?)"

        _, err := self.executeInsert(self.db, &query, flr.pd.GetPathInfoId(), flr.filename, *hash, mtime, nowEpoch)
        if err != nil {
            panic(err)
        }
    }

    return nil
}

func (self *catalogResource) createPath(relPath *string, nowEpoch int64) (id int, err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            id = 0
            err = r.(error)

            l.Error("Could not create path", "err", err)
        }
    }()

    l.Debug("Inserting path-info record.", 
        "relPath", *relPath, 
        "nowEpoch", nowEpoch)

    query := 
        "INSERT INTO `paths` " +
            "(`rel_path`, `last_check_epoch`) " +
        "VALUES " +
            "(?, ?)"

    idInt64, err := self.executeInsert(self.db, &query, *relPath, nowEpoch)
    if err != nil {
        panic(err)
    }

    l.Debug("Path record inserted.", "id", idInt64)

    return int(idInt64), nil
}

func (self *catalogResource) updatePath(pd *pathDescriptor, hash *string) (err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not update path", "err", err)
        }
    }()

    l.Debug("Updating path", 
        "relPath", pd.GetRelPath(), 
        "id", pd.GetPathInfoId(), 
        "hash", *hash)

// TODO(dustin): Can we use an alias on the table here?
    query := 
        "UPDATE " +
            "`paths` " +
        "SET " +
            "`hash` = ? " +
        "WHERE " +
            "`path_id` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    r, err := stmt.Exec(*hash, pd.GetPathInfoId())
    if err != nil {
        panic(err)
    }

    affected, err := r.RowsAffected()
    if err != nil {
        panic(err)
    }

    if affected < 1 {
        panic(errors.New("No rows were affected by the path-update query"))
    } else if affected > 1 {
        panic(errors.New("Too many rows were affected by the path-update query"))
    }

    return nil
}

// Get a list of all file records that haven't been touched in this run 
// (because all of the ones that match known files have been updated to a later 
// timestamp than they had).
func (self *catalogResource) pushOldFiles(nowEpoch int64, c chan<- *ChangeEvent) (err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not push old files", "err", err)
        }
    }()

    l.Debug("Pushing old file entries.")

    // If we're reporting changes, then enumerate the entries to be delete 
    // and push them up.

    query := 
        "SELECT " +
            "`p`.`rel_path`, " +
            "`f`.`filename` " +
        "FROM " +
            "`files` `f`, " +
            "`paths` `p` " +
        "WHERE " +
            "`f`.`last_check_epoch` < ? AND " +
            "`p`.`path_id` = `f`.`path_id`"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    rows, err := stmt.Query(nowEpoch)
    if err != nil {
        panic(err)
    }

    defer rows.Close()

    n := 0
    for rows.Next() {
        n++

        var relPath string
        var filename string

        err = rows.Scan(&relPath, &filename)
        if err != nil {
            panic(err)
        }

        relFilepath := path.Join(relPath, filename)

        c <- &ChangeEvent { 
                EntityType: EntityTypeFile, 
                ChangeType: UpdateTypeDelete, 
                RelPath: relFilepath,
        }
    }

    l.Debug("Finished reporting old file entries.", "n", n)

    return nil
}

// Get a list of all path records that haven't been touched in this run 
// (because all of the ones that match known files have been updated to a later 
// timestamp than they had).
func (self *catalogResource) pushOldPaths(nowEpoch int64, c chan<- *ChangeEvent) (err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not push old paths", "err", err)
        }
    }()

    l.Debug("Pushing old path entries.")

    // If we're reporting changes, then enumerate the entries to be delete 
    // and push them up.

    query := 
        "SELECT " +
            "`p`.`rel_path` " +
        "FROM " +
            "`paths` `p` " +
        "WHERE " +
            "`p`.`last_check_epoch` < ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    rows, err := stmt.Query(nowEpoch)
    if err != nil {
        panic(err)
    }

    defer rows.Close()

    n := 0
    for rows.Next() {
        n++

        var relPath string

        err = rows.Scan(&relPath)
        if err != nil {
            panic(err)
        }

        c <- &ChangeEvent { 
                EntityType: EntityTypePath, 
                ChangeType: UpdateTypeDelete, 
                RelPath: relPath,
        }
    }

    l.Debug("Finished reporting old path entries.", "n", n)

    return nil
}

// Delete all records that haven't been touched in this run (because all of the 
// ones that match known files have been updated to a later timestamp than they 
// had).
func (self *catalogResource) pruneOldFiles(nowEpoch int64, c chan<- *ChangeEvent) (err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not prune old files", "err", err)
        }
    }()

    if c != nil {
        err := self.pushOldFiles(nowEpoch, c)
        if err != nil {
            panic(err)
        }
    }

    query := 
        "DELETE FROM `files` " +
        "WHERE " +
            "`last_check_epoch` < ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    r, err := stmt.Exec(nowEpoch)
    if err != nil {
        panic(err)
    }

    affected, err := r.RowsAffected()
    if err != nil {
        panic(err)
    }

    l.Debug("Pruned old FILE entries.", "affected", affected)

    return nil
}

func (self *catalogResource) pruneOldPaths(nowEpoch int64, c chan<- *ChangeEvent) (err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            err = r.(error)
            l.Error("Could not prune old paths", "err", err)
        }
    }()

    if c != nil {
        err := self.pushOldPaths(nowEpoch, c)
        if err != nil {
            panic(err)
        }
    }

    query := 
        "DELETE FROM `paths` " +
        "WHERE " +
            "`last_check_epoch` < ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    r, err := stmt.Exec(nowEpoch)
    if err != nil {
        panic(err)
    }

    affected, err := r.RowsAffected()
    if err != nil {
        panic(err)
    }

    l.Debug("Pruned old PATH entries.", "affected", affected)

    return nil
}

func (self *catalogResource) getLastPathHash(relPath *string) (hp *string, err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            hp = nil
            err = r.(error)

            l.Error("Could not get last path hash", "err", err)
        }
    }()

    query := 
        "SELECT " +
            "`p`.`hash` " +
        "FROM " +
            "`paths` `p` " +
        "WHERE " +
            "`p`.`rel_path` = ?"

    stmt, err := self.db.Prepare(query)
    if err != nil {
        panic(err)
    }

    rows, err := stmt.Query(*relPath)
    if err != nil {
        panic(err)
    }

    defer rows.Close()

    if rows.Next() == false {
        panic(err)
    }

    var hash string

    err = rows.Scan(&hash)
    if err != nil {
        panic(err)
    }

    return &hash, nil
}

type resolveResult struct {
    RelPath string
    PathId int
    Filename string
    FileId int
    Hash string
}

func (self *catalogResource) ResolvePath(relPath *string) (rr *resolveResult, err error) {
    l := NewLogger("catalog-resource")

    defer func() {
        if r := recover(); r != nil {
            rr = nil
            err = r.(error)

            l.Error("Could not resolve the path", "err", err)
        }
    }()

    plr, err := self.lookupPath(relPath)
    if err != nil {
        panic(err)
    }

    if plr.wasFound == false {
        // We weren't given a [valid] path. Try it as a file.

        if *relPath == "" {
            panic(errors.New("We're looking for the root hash but it isn't recorded."))
        }

        parentPath := path.Dir(*relPath)
        filename := path.Base(*relPath)

        plr, err := self.lookupPath(&parentPath)
        if err != nil {
            panic(err)
        } else if plr.wasFound == false {
            panic(errors.New("Argument not found as path or file."))
        }

        pd := newRecordedPathDescriptor(&parentPath, plr.entry.id)

        flr, err := self.lookupFile(pd, &filename)
        if err != nil {
            panic(err)
        } else if plr.wasFound == false {
            panic(errors.New("Parent directory found but the argument wasn't found as a file within it."))
        }

        rr = &resolveResult {
                RelPath: parentPath,
                PathId: plr.entry.id,
                Filename: filename,
                FileId: flr.entry.id,
                Hash: flr.entry.hash,
        }
    } else {
        rr = &resolveResult {
                RelPath: *relPath,
                PathId: plr.entry.id,
                Hash: plr.entry.hash,
        }
    }

    return rr, nil
}
