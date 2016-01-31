This tool will recursively and efficiently calculate a SHA1 hash for a given directory. Subsequent checks will not recalculate any file hashes unless any files have changed. You are required to provide a file-path to write the file catalog to.


## Usage

```
$ pfhash -s <scan path> -c <catalog file-path>
```

The catalog file-path does not have to already exist.


## Example

Calculate the hashes on an expensive (hundreds of directories, tens of thousands of files with an average size of ~5M) path:

```
$ time pfhash -s photos_path -c catalog_file
8250cf94b55e106ce48a83a15569b866aecc1183

real    36m59.201s
user    6m47.892s
sys     3m24.316s
```

Run it again and see the savings:

```
$ time pfhash -s photos_path -c catalog_file
8250cf94b55e106ce48a83a15569b866aecc1183

real    3m16.700s
user    0m8.928s
sys     0m8.112s
```

If you positively don't want to update the hashes nor do you want a report of changes, use the `pflookup` command:

```
$ pflookup -c catalog_filepath
8250cf94b55e106ce48a83a15569b866aecc1183

$ pflookup -c catalog_filepath -r subdir1
722ac04c963e16f39655fd4ea0a428ff32ba8399

$ pflookup -c catalog_filepath -r subdir1/aa
da39a3ee5e6b4b0d3255bfef95601890afd80709
```

The *second* form just provides a specific subdirectory that you want the hash for. By default, it returns for the root. The *third* form is similar, but, in this case, we're looking up the hash for a specific file.


## Dependencies

- Go 1.5+
- Mercurial


## Install

$ make
go get pathfingerprint/pfhash
go get pathfingerprint/pflookup
go test pathfingerprint/pfinternal
ok    pathfingerprint/pfinternal  0.015s

$ sudo make install
install -m 755 bin/pfhash /usr/local/bin/pfhash
install -m 755 bin/pflookup /usr/local/bin/pflookup


## Other Features

### Reporting

You can tell the tool to write a file with all detected changes (or "-" for STDERR). This file will looks like:

```
$ mkdir -p scan_path/subdir1
$ mkdir -p scan_path/subdir2
$ touch scan_path/subdir1/aa
$ touch scan_path/subdir1/bb

$ pfhash -s scan_path -c catalog_file -R - 
create file subdir1/aa
create file subdir1/bb
create path subdir1
create path subdir2
create path .
f52422e037072f73d5d0c3b1ab2d51e3edf67cf3

$ touch scan_path/subdir1/aa
$ touch scan_path/subdir2/new_file

$ pfhash -s scan_path -c catalog_file -R - 
update file subdir1/aa
create file subdir2/new_file
update path subdir2
update path .
8250cf94b55e106ce48a83a15569b866aecc1183
```

Note the "create path ." remark. This is shown because the root catalog didn't previously exist.


### No-Updates Mode

The catalog will usually be updated whether it's the first time you calculate a hash or subsequent times. As mentioned in the implementation notes, we need to do this in order to determine when files have been deleted. You can pass the parameter to prevent updates from being made (in the event that the catalog has been stored on a read-only mount, for example), but, if you've requested a changes report, this will cause deletions to be omitted from the report.

**This feature is still experimental.**


## Implementation Notes

- The catalog is a SQLite database.
- We use the cached file hashes to skip recalculation whenever possible but we recalculate path hashes every time since we still can't avoid checking every file.
- We determine if a file hash should be recalculated based on modified-times but the hash does not implemented the modified-time: If you accidentally affect a file's mtime without actually changing the file, the hash will stay constant.
- The catalog is meant to be portable. You are able to move the contents of the scan-path and the contents of the catalog to a different place without affecting the hashes that are generated. You might use this fact to:
  - archive the catalog and keep it in the root of whatever directory it represents
  - keep a backup of your catalogs on a separate disk
  - ship a copy of your files to offsite backup while keeping a local copy of your catalog for reference
  - etc..
- As we check a certain path for changes, we update a check-timestamp on each file in that catalog with a new timestamp. We then delete all entries older than that timestamp when we're done processing that directory. This efficiently allows us to both check differences *and* keep the catalog up to date.
- Because we can't determine which directories or files have been removed until the end of the process, deleted directories and files are listed at the bottom of the change report. Because we write updates as we encounter them, you'll see new directory events appear before the files that appear within them, and then update events for that directory after. 


## Advanced Usage

If you feel compelled, you can inspect the catalogs yourself.

```
$ pfhash -s scan_path -c catalog_file -R - 
create file subdir1/aa
create file subdir1/bb
create path subdir1
create path subdir2
create path .
f52422e037072f73d5d0c3b1ab2d51e3edf67cf3
```

To look at the catalog, install and use the SQLite 3 command-line tool to open the catalog.

```
$ sqlite3 catalog_file
SQLite version 3.8.2 2013-12-06 14:53:30
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
```

There are two tables: One that tracks the information for the paths (`paths`) and a table that tracks files (`files`):

```
sqlite> .schema
CREATE TABLE `catalog_info` (
`catalog_info_id` INTEGER NOT NULL PRIMARY KEY, 
`key` VARCHAR(50) NOT NULL UNIQUE, 
`value` VARCHAR(200) NULL 
);

CREATE TABLE `paths` (
`path_id` INTEGER NOT NULL PRIMARY KEY, 
`rel_path` VARCHAR(1000) NOT NULL, 
`hash` VARCHAR(40) NULL, 
`last_check_epoch` INTEGER UNSIGNED NULL DEFAULT 0, 
CONSTRAINT `paths_rel_path_idx` UNIQUE (`rel_path`)
);

CREATE INDEX paths_last_check_epoch_idx ON `paths`(`last_check_epoch` ASC);

CREATE TABLE `files` (
`file_id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
`path_id` INTEGER NOT NULL, 
`filename` VARCHAR(255) NOT NULL, 
`hash` VARCHAR(40) NOT NULL, 
`mtime_epoch` INTEGER UNSIGNED NOT NULL, 
`last_check_epoch` INTEGER UNSIGNED NULL DEFAULT 0, 
CONSTRAINT `files_filename_idx` UNIQUE (`filename`, `path_id`), 
CONSTRAINT `files_path_id_fk` FOREIGN KEY (`path_id`) REFERENCES `paths` (`path_id`)
);

CREATE INDEX files_last_check_epoch_idx ON `files`(`last_check_epoch` ASC);

sqlite> select * from paths;
1||6aa8497382567423b54cf5df5219b7a919bcd852|1|1454263914
2|dir1|cf2474d380f31b1000bbfa2c3ba8f4d5dfa3f911|1|1454263914
3|dir1/dir1dir1|013717fdca5c76331fbcb02e166d775dd6c5e34f|1|1454263914
4|dir2|18b040d8a968fa875002cd573c476ab9738501ba|1|1454263914

sqlite> select * from files;
1|1|aa|da39a3ee5e6b4b0d3255bfef95601890afd80709|1454205021|1454263914
2|1|bb|da39a3ee5e6b4b0d3255bfef95601890afd80709|1454205022|1454263914
3|2|cc|90cda474cb6daddeb084c0f58abe41b26f418e8f|1454262735|1454263914
...
```

To see the last hash that was generated for the root directory, look at the hash for the corresponding record in the `paths` table:

```
$ sqlite3 catalog_file
SQLite version 3.8.2 2013-12-06 14:53:30
Enter ".help" for instructions
Enter SQL statements terminated with a ";"

sqlite> select hash from paths where rel_path = "";
6aa8497382567423b54cf5df5219b7a919bcd852
```


## Command-Line Options

### pfhash

```
$ pfhash -h
Usage:
  pfhash [OPTIONS]

Application Options:
  -s, --scan-path=        Path to scan
  -c, --catalog-filepath= Catalog file-path (will be created if it doesn't exist)
  -h, --algorithm=        Hashing algorithm (sha1, sha256) (default: sha1)
  -n, --no-updates        Don't update the catalog (will also prevent reporting of deletions) (default: false)
  -R, --report=           Write a report of changed files ('-' for STDERR)
  -P, --profile=          Write performance profiling information
  -d, --debug-log         Show debug logging (default: false)

Help Options:
  -h, --help              Show this help message
```


### pflookup

```
$ pflookup -h
Usage:
  pflookup [OPTIONS]

Application Options:
  -c, --catalog-filepath= Catalog path
  -h, --algorithm=        Hashing algorithm (sha1, sha256) (default: sha1)
  -d, --debug-log         Show debug logging (default: false)
  -r, --rel-path=         Specific subdirectory

Help Options:
  -h, --help              Show this help message
```
