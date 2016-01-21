package main

import (
    "os"
    "fmt"
    
    flags "github.com/jessevdk/go-flags"

    "pathfingerprint/pfinternal"
)

type options struct {
    CatalogPath string      `short:"c" long:"catalog-path" description:"Path to host catalog (will be created if it doesn't exist)" required:"true"`
    HashAlgorithm string    `short:"h" long:"algorithm" default:"sha1" description:"Hashing algorithm (sha1, sha256)"`
    ShowDebugLogging bool   `short:"d" long:"debug-log" default:"false" description:"Show debug logging"`
    RecallRelPath string    `short:"p" long:"recall-rel-path" default:"" description:"If we're recalling, lookup for a specific subdirectory"`
}

func readOptions () *options {
    o := options {}

    _, err := flags.Parse(&o)
    if err != nil {
        os.Exit(1)
    }

    return &o
}

func main() {
    var catalogPath string
    var hashAlgorithm string
    var recallRelPath string

    o := readOptions()

    catalogPath = o.CatalogPath
    hashAlgorithm = o.HashAlgorithm
    recallRelPath = o.RecallRelPath

    if o.ShowDebugLogging == true {
        pfinternal.SetDebugLogging()
    }

    l := pfinternal.NewLogger("pflookup")
    l.ConfigureRootLogger()

    var effectiveRelPath *string

    if recallRelPath != "" {
        effectiveRelPath = &recallRelPath
    } else {
        effectiveRelPath = nil
    }

    hash, err := pfinternal.RecallHash(&catalogPath, effectiveRelPath, &hashAlgorithm)
    if err != nil {
        l.Error("Could not recall the hash", "error", err.Error())
        os.Exit(2)
    }

    fmt.Printf("%s\n", *hash)
}
