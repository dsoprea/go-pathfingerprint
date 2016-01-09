This tool will recursively and efficiently calculate a SHA1 hash for a given directory. Subsequent checks will not recalculate any file hashes unless any files have changed. You are required to provide a catalog path to store indexing information in. 

Usage:

```
pathfingerprint <scan path> <catalog path>
```

The catalog path does not have to already exist.

Example:

```
$ time pathfingerprint photos_path photos_catalog
953c294b09774384873f15a8a846a887c9ffdc0b

real    2m14.118s
user    0m55.420s
sys     1m1.570s

$ time pathfingerprint photos_path photos_catalog
953c294b09774384873f15a8a846a887c9ffdc0b

real    0m1.971s
user    0m0.418s
sys     0m0.873s
```

Implementation:

- A SQLite database is used to index every directory.
- We cache file hashes but not path hashes.
- We determine if a file hash changes based on modified-times.

## Dependencies

- Go 1.5+
- Mercurial

## Remaining Tasks

- Allow arbitrary choice of hashes
- Allow to disable updates via command-line
- Add a reporting mechanism to either print or record deltas
