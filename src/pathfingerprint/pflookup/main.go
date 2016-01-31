package main

import (
    "os"
    "fmt"
    
    flags "github.com/jessevdk/go-flags"

    "pathfingerprint/pfinternal"
)

type options struct {
    CatalogFilepath string  `short:"c" long:"catalog-filepath" description:"Catalog file-path" required:"true"`
    HashAlgorithm string    `short:"h" long:"algorithm" default:"sha1" description:"Hashing algorithm (sha1, sha256)"`
    ShowDebugLogging bool   `short:"d" long:"debug-log" default:"false" description:"Show debug logging"`
    ShowExtended bool       `short:"e" long:"show-extended" default:"false" description:"Show extended info"`
    RelPath string          `short:"r" long:"rel-path" default:"" description:"Specific subdirectory"`
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
    defer func() {
        if r := recover(); r != nil {
            err := r.(error)

            fmt.Printf("ERROR: %s\n", err.Error())
            os.Exit(1)
        }
    }()

    var catalogFilepath string
    var hashAlgorithm string
    var relPath string
    var showExtended bool

    o := readOptions()

    catalogFilepath = o.CatalogFilepath
    hashAlgorithm = o.HashAlgorithm
    relPath = o.RelPath
    showExtended = o.ShowExtended

    if o.ShowDebugLogging == true {
        pfinternal.SetDebugLogging()
    }

    pfinternal.ConfigureRootLogger()

    cr, err := pfinternal.NewCatalogResource(&catalogFilepath, &hashAlgorithm)
    if err != nil {
        panic(err)
    }

    err = cr.Open()
    if err != nil {
        panic(err)
    }

    defer cr.Close()

    rr, err := cr.ResolvePath(&relPath)
    if err != nil {
        panic(err)
    }

    if showExtended == true {
        fmt.Printf("Path name: [%s]\n", rr.RelPath)
        fmt.Printf("Path ID: (%d)\n", rr.PathId)
        fmt.Printf("File name: [%s]\n", rr.Filename)
        fmt.Printf("File ID: [%s]\n", rr.FileId)
        fmt.Printf("Hash: [%s]\n", rr.Hash)
    } else {
        fmt.Println(rr.Hash)
    }
}
