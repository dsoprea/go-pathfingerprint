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

func createTempFile(path string) string {
    f, err := ioutil.TempFile(path, "pftest_")
    if err != nil {
        panic(err)
    }

    defer f.Close()

    return f.Name()
}

func createSpecificFile(inPath string, filename string) {
    filepath := path.Join(inPath, filename)

    f, err := os.Create(filepath)
    if err != nil {
        panic(err)
    }

    f.Close()
}

func createTempPath(parentPath string, prefix string) string {
    finalPath, err := ioutil.TempDir(parentPath, prefix)
    if err != nil {
        panic(err)
    }

    return finalPath
}

func TestCalculateSimpleHash(t *testing.T) {
    ConfigureRootLogger()

    hashAlgorithm := HashAlgorithm

    p := NewPath(&hashAlgorithm, nil)

    tempPath := os.TempDir()
    catalogFilepath := createTempFile(tempPath)
    scanPath := createTempPath(tempPath, "scan")

    createSpecificFile(scanPath, "aa")
    createSpecificFile(scanPath, "bb")
    createSpecificFile(scanPath, "cc")

    cleanup := func() {
        os.RemoveAll(scanPath)
        os.Remove(catalogFilepath)
    }

    defer cleanup()

    cr, err := NewCatalogResource(&catalogFilepath, &hashAlgorithm)
    if err != nil {
        t.Fatalf("Could not create catalog-resource.")
    }

    err = cr.Open()
    if err != nil {
        panic(err)
    }

    defer cr.Close()

    c, err := NewCatalog(cr, &scanPath, true, &hashAlgorithm, nil)
    if err != nil {
        t.Fatalf("Could not create new catalog.")
    }

    relPath := ""
    hash, err := p.GeneratePathHash(&scanPath, &relPath, c)
    if err != nil {
        t.Fatalf("Could not generate hash.")
    }

    expectedHash := "7de9894aa603d20dae695e2a9bccf02d465979cb"

    if hash != expectedHash {
        t.Fatalf("Hash was not generated correctly: ACT [%s] != EXP [%s].", hash, expectedHash)
    }
}
