package main

import (
    "os"
    "fmt"
    "runtime/pprof"
    
    flags "github.com/jessevdk/go-flags"

    "pathfingerprint/pfinternal"
)

type options struct {
    ScanPath string         `short:"s" long:"scan-path" description:"Path to scan" required:"true"`
    CatalogPath string      `short:"c" long:"catalog-path" description:"Path to host catalog (will be created if it doesn't exist)" required:"true"`
    HashAlgorithm string    `short:"h" long:"algorithm" default:"sha1" description:"Hashing algorithm (sha1, sha256)"`
    NoUpdates bool          `short:"n" long:"no-updates" default:"false" description:"Don't update the catalog (will also prevent reporting of deletions)"`
    ReportFilename string   `short:"r" long:"report" default:"" description:"Write a report of changed files"`
    ProfileFilename string  `short:"p" long:"profile" default:"" description:"Write performance profiling information"`
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
    var scanPath string
    var catalogPath string
    var hashAlgorithm string
    var allowUpdates bool
    var reportFilename string
    var profileFilename string

    o := readOptions()

    scanPath = o.ScanPath
    catalogPath = o.CatalogPath
    hashAlgorithm = o.HashAlgorithm
    allowUpdates = o.NoUpdates == false
    reportFilename = o.ReportFilename
    profileFilename = o.ProfileFilename

    var reportingDataChannel chan *pfinternal.CatalogChange = nil
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
            l.DieIf(err, "Could not create profiler profile.")
        }

        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

    p := pfinternal.NewPath(&hashAlgorithm)

    if reportFilename != "" {
        reportingDataChannel = make(chan *pfinternal.CatalogChange, 1000)
        reportingQuitChannel = make(chan bool)

        go recordChanges(reportFilename, reportingDataChannel, reportingQuitChannel)
    }

    c, err = pfinternal.NewCatalog(&catalogPath, &scanPath, allowUpdates, &hashAlgorithm, reportingDataChannel)
    if err != nil {
        l.Error("Could not open catalog.", "error", err.Error())
        os.Exit(1)
    }

    hash, err := p.GeneratePathHash(&scanPath, c)
    if err != nil {
        l.Error("Could not generate hash.", "error", err.Error())
        os.Exit(2)
    }

    if reportFilename != "" {
        reportingQuitChannel <- true
    }

    fmt.Printf("%s\n", hash)
}

func recordChanges (reportFilename string, reportingChannel <-chan *pfinternal.CatalogChange, reportingQuit <-chan bool) {
    l := pfinternal.NewLogger("pfmain")

    f, err := os.Create(reportFilename)
    if err != nil {
        l.Error("Could not open report file.", "filename", reportFilename, "error", err.Error())
        panic(err)
    }

    defer f.Close()

    l.Debug("Reporter running.")

    for {
        select {
            case change := <-reportingChannel:
                l.Debug("Catalog change.", "ChangeType", pfinternal.CatalogEntryUpdateTypes[change.ChangeType], "RelFilepath", *change.RelFilepath)
                f.WriteString(pfinternal.CatalogEntryUpdateTypes[change.ChangeType])
                f.WriteString(" ")
                f.WriteString(*change.RelFilepath)
                f.WriteString("\n")
            case <-reportingQuit:
                l.Debug("Reporting terminating.")
                return

        }
    }
}
