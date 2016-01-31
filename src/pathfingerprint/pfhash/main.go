package main

import (
    "os"
    "fmt"
    "runtime/pprof"
//    "runtime"
    
    flags "github.com/jessevdk/go-flags"

    "pathfingerprint/pfinternal"
)

const (
    PathCreationMode = 0755
)

type options struct {
    ScanPath string         `short:"s" long:"scan-path" description:"Path to scan" required:"true"`
    CatalogFilepath string  `short:"c" long:"catalog-filepath" description:"Catalog file-path (will be created if it doesn't exist)" required:"true"`
    HashAlgorithm string    `short:"h" long:"algorithm" default:"sha1" description:"Hashing algorithm (sha1, sha256)"`
    NoUpdates bool          `short:"n" long:"no-updates" default:"false" description:"Don't update the catalog (will also prevent reporting of deletions)"`
    ReportFilename string   `short:"R" long:"report" default:"" description:"Write a report of changed files ('-' for STDERR)"`
    ProfileFilename string  `short:"P" long:"profile" default:"" description:"Write performance profiling information"`
    ShowDebugLogging bool   `short:"d" long:"debug-log" default:"false" description:"Show debug logging"`
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

    var scanPath string
    var catalogFilepath string
    var hashAlgorithm string
    var allowUpdates bool
    var reportFilename string
    var profileFilename string

    o := readOptions()

    scanPath = o.ScanPath
    catalogFilepath = o.CatalogFilepath
    hashAlgorithm = o.HashAlgorithm
    allowUpdates = o.NoUpdates == false
    reportFilename = o.ReportFilename
    profileFilename = o.ProfileFilename

    var reportingDataChannel chan *pfinternal.ChangeEvent = nil
    var reportingQuitChannel chan bool = nil
    var c *pfinternal.Catalog
    var err error

    if o.ShowDebugLogging == true {
        pfinternal.SetDebugLogging()
    }

    l := pfinternal.NewLogger("pfhash")
    pfinternal.ConfigureRootLogger()

    if profileFilename != "" {
        l.Debug("Profiling enabled.")

        f, err := os.Create(profileFilename)
        if err != nil {
            panic(err)
        }

//        runtime.SetCPUProfileRate(100)
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

    if reportFilename != "" {
        reportingDataChannel = make(chan *pfinternal.ChangeEvent, 1000)
        reportingQuitChannel = make(chan bool)

        go recordChanges(reportFilename, reportingDataChannel, reportingQuitChannel)
    }

    p := pfinternal.NewPath(&hashAlgorithm, reportingDataChannel)

    cr, err := pfinternal.NewCatalogResource(&catalogFilepath, &hashAlgorithm)
    if err != nil {
        panic(err)
    }

    err = cr.Open()
    if err != nil {
        panic(err)
    }

    defer cr.Close()

    c, err = pfinternal.NewCatalog(cr, &scanPath, allowUpdates, &hashAlgorithm, reportingDataChannel)
    if err != nil {
        panic(err)
    }

    err = c.Open()
    if err != nil {
        panic(err)
    }

    defer c.Close()

    l.Debug("Generating root hash.")

    relPath := ""
    hash, err := p.GeneratePathHash(&scanPath, &relPath, c)
    if err != nil {
        panic(err)
    }

    c.Cleanup()

    if reportFilename != "" {
        reportingQuitChannel <- true
    }

    fmt.Printf("%s\n", hash)
}

func recordChanges (reportFilename string, reportingChannel <-chan *pfinternal.ChangeEvent, reportingQuit <-chan bool) {
    l := pfinternal.NewLogger("recordChanges")

    var f *os.File
    if reportFilename == "-" {
        f = os.Stderr
    } else {
        f, err := os.Create(reportFilename)
        if err != nil {
            panic(err)
        }

        defer f.Close()
    }

    l.Debug("Reporter running.")

    for {
        select {
            case change := <-reportingChannel:
                changeTypeName := pfinternal.UpdateTypeName(change.ChangeType)
                entityTypeName := pfinternal.EntityTypeName(change.EntityType)

                l.Debug("Catalog change.", "EntityType", entityTypeName, "ChangeType", changeTypeName, "RelPath", change.RelPath)

                var effectiveRelPath string
                if change.EntityType == pfinternal.EntityTypePath && change.RelPath == "" {
                    effectiveRelPath = "."
                } else {
                    effectiveRelPath = change.RelPath
                }

                f.WriteString(changeTypeName)
                f.WriteString(" ")
                f.WriteString(entityTypeName)
                f.WriteString(" ")
                f.WriteString(effectiveRelPath)
                f.WriteString("\n")

            case <-reportingQuit:
                return
        }
    }
}
