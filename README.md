This tool will recursively and efficiently calculate a SHA1 hash for a given directory. Subsequent checks will not recalculate any file hashes unless any files have changed. You are required to provide a catalog path to store indexing information in. 


## Usage

```
$ pathfingerprint -s <scan path> -c <catalog path>
```

The catalog path does not have to already exist.


## Dependencies

- Go 1.5+
- Mercurial


## Example

```
$ time pathfingerprint -s photos_path -c catalog_path
4767c85a1743ea88a31caca90c3f23cdbef30471

real    36m59.201s
user    6m47.892s
sys     3m24.316s

$ time pathfingerprint -s photos_path -c catalog_path
4767c85a1743ea88a31caca90c3f23cdbef30471

real    3m16.700s
user    0m8.928s
sys     0m8.112s
```


## Other Features

### Reporting

You can tell the tool to write a file with all detected changes. This file will looks like:

```
pathfingerprint -s scan_path -c catalog_path -r - 
create file subdir1/aa
create file subdir1/bb
create path subdir1
create path subdir2
create path .
f52422e037072f73d5d0c3b1ab2d51e3edf67cf3
```

Note the "create path ." remark. This is shown because the root catalog didn't previously exist.

### No-Updates Mode

The catalog will usually be updated whether it's the first time you calculate a hash or subsequent times. As mentioned in the implementation notes, we need to do this in order to determine when files have been deleted. You can pass the parameter to prevent updates from being made (in the event that the catalog has been stored on a read-only mount, for example), but, if you've requested a changes report, this will cause deletions to be omitted from the report.


## Implementation Notes

- A SQLite database is used to index each directory. These are deposited into the catalog-path.
- We cache file hashes but not path hashes.
- We determine if a file hash changes based on modified-times.
- As we check a certain path for changes, we update a check-timestamp on each file in that catalog with a new timestamp. We then delete all entries older than that timestamp when we're done processing that directory. This efficiently allows us to both check differences *and* keep the catalog up to date.
- Because we open and close a database for each path, it's far more efficient to process a directory structure with many files and not as much when there are many empty or under-utilized directories as compared to files.


## Advanced Usage

If you feel compelled, you can inspect the catalogs yourself.

```
$ pathfingerprint -s scan_path -c catalog_path -r - 
create file subdir1/aa
create file subdir1/bb
create path subdir1
create path subdir2
create path .
f52422e037072f73d5d0c3b1ab2d51e3edf67cf3
```

To look at the catalog for a particular path, calculate a SHA1 for the relative path name:

```
$ echo -n subdir1 | sha1sum | awk '{print $1}'
84996436541614ee0a22f04a32d22d45407c4a42
```

Then, install and use the SQLite 3 command-line tool to open the file named for that hash in the catalog-path.

```
$ sqlite3 catalog_path/84996436541614ee0a22f04a32d22d45407c4a42
SQLite version 3.8.2 2013-12-06 14:53:30
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
```

There are two tables: One that tracks the information for that path (`path_info`; there will only be one entry) and a table that tracks file entries (`catalog_entries`):

```
sqlite> .schema
CREATE TABLE `path_info` (`path_info_id` INTEGER NOT NULL PRIMARY KEY, `rel_path` VARCHAR(1000) NOT NULL, `hash` VARCHAR(40) NOT NULL );
CREATE TABLE `catalog_entries` (`catalog_entry_id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, `filename` VARCHAR(255) NOT NULL, `hash` VARCHAR(40) NOT NULL, `mtime_epoch` INTEGER UNSIGNED NOT NULL, `last_check_epoch` INTEGER UNSIGNED NULL DEFAULT 0, CONSTRAINT `filename_idx` UNIQUE (`filename`));

sqlite> select * from path_info;
1|subdir1|722ac04c963e16f39655fd4ea0a428ff32ba8399

sqlite> select * from catalog_entries;
1|aa|da39a3ee5e6b4b0d3255bfef95601890afd80709|1453343619|1453343628
2|bb|da39a3ee5e6b4b0d3255bfef95601890afd80709|1453343620|1453343628
```

The root catalog is simply named "root". To see the last hash that was generated, look at the hash for the single record in the `path_info` table.

```
$ sqlite3 catalog_path/root
SQLite version 3.8.2 2013-12-06 14:53:30
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
sqlite> select * from path_info;
1||f52422e037072f73d5d0c3b1ab2d51e3edf67cf3
```


## Command-Line Options

$ pathfingerprint -h
Usage:
  pathfingerprint [OPTIONS]

Application Options:
  -s, --scan-path=    Path to scan
  -c, --catalog-path= Path to host catalog (will be created if it doesn't exist)
  -h, --algorithm=    Hashing algorithm (sha1, sha256) (default: sha1)
  -n, --no-updates    Don't update the catalog (will also prevent reporting of deletions) (default: false)
  -r, --report=       Write a report of changed files ('-' for STDERR)
  -p, --profile=      Write performance profiling information
  -d, --debug-log     Show debug logging (default: false)

Help Options:
  -h, --help          Show this help message
