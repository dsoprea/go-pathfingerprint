package pfinternal

import (
    "testing"
    "os"
    "path"
    "io/ioutil"
)

const (
    HashAlgorithm = "sha1"
)

func createFile (path *string) string {
    f, err := ioutil.TempFile(*path, "entry")
    if err != nil {
        panic(err)
    }

    defer f.Close()

    return f.Name()
}

func createSpecificFile (inPath *string, filename string) {
    filepath := path.Join(*inPath, filename)

    f, err := os.Create(filepath)
    if err != nil {
        panic(err)
    }

    f.Close()
}

func TestCalculateSimpleHash (t *testing.T) {
    l := NewLogger("path_test")
    l.ConfigureRootLogger()

    hashAlgorithm := HashAlgorithm

    p := NewPath(&hashAlgorithm, nil)

    tempPath := os.TempDir()
    catalogPath, err := ioutil.TempDir(tempPath, "catalog")
    if err != nil {
        t.Fatalf("Could not create temp path (1).")
    }

    scanPath, err := ioutil.TempDir(tempPath, "scan")
    if err != nil {
        t.Fatalf("Could not create temp path (2).")
    }

    createSpecificFile(&scanPath, "aa")
    createSpecificFile(&scanPath, "bb")
    createSpecificFile(&scanPath, "cc")

    cleanup := func() {
        os.RemoveAll(scanPath)
        os.RemoveAll(catalogPath)
    }

    defer cleanup()

    c, err := NewCatalog(&catalogPath, &scanPath, true, &hashAlgorithm, nil)
    if err != nil {
        t.Fatalf("Could not create catalog.")
    }

    hash, err := p.GeneratePathHash(&scanPath, c)
    if err != nil {
        t.Fatalf("Could not generate hash.")
    }

    expectedHash := "e58842ca5e5c852751a5c686084293829b716a8b"

    if hash != expectedHash {
        t.Fatalf("Hash was not generated correctly: ACT [%s] != EXP [%s].", hash, expectedHash)
    }
}
