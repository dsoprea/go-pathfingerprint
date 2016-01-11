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
$ pathfingerprint -s /tmp/scan_path -c /tmp/catalog_path -r changes.txt 
57d947b0b82ec79182633e8572c7e5c74748dc93

$ cat changes.txt 
insert subdir/created_file
update subdir/updated_file
delete subdir/deleted_file
```

### No-Updates Mode

The catalog will usually be updated whether it's the first time you calculate a hash or subsequent times. As mentioned in the implementation notes, we need to do this in order to determine when files have been deleted. You can pass the parameter to prevent updates from being made (in the event that the catalog has been stored on a read-only mount, for example), but, if you've requested a changes report, this will cause deletions to be omitted from the report.


## Implementation Notes

- A SQLite database is used to index each directory. These are deposited into the catalog-path.
- We cache file hashes but not path hashes.
- We determine if a file hash changes based on modified-times.
- As we check a certain path for changes, we update a check-timestamp on each file in that catalog with a new timestamp. We then delete all entries older than that timestamp when we're done processing that directory. This efficiently allows us to both check differences *and* keep the catalog up to date.


## Command-Line Options

$ pathfingerprint -h
Usage:
  pathfingerprint [OPTIONS]

Application Options:
  -s, --scan-path=    Path to scan
  -c, --catalog-path= Path to host catalog (will be created if it doesn't exist)
  -h, --algorithm=    Hashing algorithm (sha1, sha256) (default: sha1)
  -n, --no-updates    Don't update the catalog (will also prevent reporting of deletions) (default: false)
  -r, --report=       Write a report of changed files
  -p, --profile=      Write performance profiling information
  -d, --debug-log     Show debug logging (default: false)

Help Options:
  -h, --help          Show this help message
