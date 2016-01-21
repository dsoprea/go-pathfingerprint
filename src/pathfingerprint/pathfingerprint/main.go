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
    CatalogPath string      `short:"c" long:"catalog-path" description:"Path to host catalog (will be created if it doesn't exist)" required:"true"`
    HashAlgorithm string    `short:"h" long:"algorithm" default:"sha1" description:"Hashing algorithm (sha1, sha256)"`
    NoUpdates bool          `short:"n" long:"no-updates" default:"false" description:"Don't update the catalog (will also prevent reporting of deletions)"`
    ReportFilename string   `short:"R" long:"report" default:"" description:"Write a report of changed files ('-' for STDERR)"`
    ProfileFilename string  `short:"P" long:"profile" default:"" description:"Write performance profiling information"`
    ShowDebugLogging bool   `short:"d" long:"debug-log" default:"false" description:"Show debug logging"`
    RecallOnly bool         `short:"r" long:"recall" default:"false" description:"Lookup the last calculated hash (don't recalculate)"`
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
    var scanPath string
    var catalogPath string
    var hashAlgorithm string
    var allowUpdates bool
    var reportFilename string
    var profileFilename string
    var recallOnly bool
    var recallRelPath string

    o := readOptions()

    scanPath = o.ScanPath
    catalogPath = o.CatalogPath
    hashAlgorithm = o.HashAlgorithm
    allowUpdates = o.NoUpdates == false
    reportFilename = o.ReportFilename
    profileFilename = o.ProfileFilename
    recallOnly = o.RecallOnly
    recallRelPath = o.RecallRelPath

    var reportingDataChannel chan *pfinternal.ChangeEvent = nil
    var reportingQuitChannel chan bool = nil
    var c *pfinternal.Catalog
    var err error

    if o.ShowDebugLogging == true {
        pfinternal.SetDebugLogging()
    }

    l := pfinternal.NewLogger("pfmain")
    l.ConfigureRootLogger()

    if profileFilename != "" {
        l.Info("Profiling enabled.")

        f, err := os.Create(profileFilename)
        if err != nil {
            l.Error("Could not create profiler profile.", "error", err.Error())
            os.Exit(1)
        }

//        runtime.SetCPUProfileRate(100)
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

    if recallOnly == true {
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
        return
    }

    if reportFilename != "" {
        reportingDataChannel = make(chan *pfinternal.ChangeEvent, 1000)
        reportingQuitChannel = make(chan bool)

        go recordChanges(reportFilename, reportingDataChannel, reportingQuitChannel)
    }

    err = os.MkdirAll(catalogPath, PathCreationMode)
    if err != nil {
        l.Error("Could not create catalog path", "error", err.Error())
        os.Exit(3)
    }

    p := pfinternal.NewPath(&hashAlgorithm, reportingDataChannel)

    c, err = pfinternal.NewCatalog(&catalogPath, &scanPath, allowUpdates, &hashAlgorithm, reportingDataChannel)
    if err != nil {
        l.Error("Could not create catalog.", "error", err.Error())
        os.Exit(4)
    }

    hash, err := p.GeneratePathHash(&scanPath, c)
    if err != nil {
        l.Error("Could not generate hash.", "error", err.Error())
        os.Exit(5)
    }

    if allowUpdates == true {
        err = c.PruneOldCatalogs()
        if err != nil {
            l.Error("Could not prune old catalogs.", "error", err.Error())
            os.Exit(6)
        }
    }

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
            l.DieIf(err, "Could not open report file.", "filename", reportFilename)
        }

        defer f.Close()
    }

    l.Debug("Reporter running.")

    for {
        select {
            case change := <-reportingChannel:
                l.Debug("Catalog change.", "EntityType", *change.EntityType, "ChangeType", *change.ChangeType, "RelPath", *change.RelPath)

                var effectiveRelPath string
                if *change.EntityType == pfinternal.EntityTypePath && *change.RelPath == "" {
                    effectiveRelPath = "."
                } else {
                    effectiveRelPath = *change.RelPath
                }

                f.WriteString(*change.ChangeType)
                f.WriteString(" ")
                f.WriteString(*change.EntityType)
                f.WriteString(" ")
                f.WriteString(effectiveRelPath)
                f.WriteString("\n")

            case <-reportingQuit:
                return
        }
    }
}
