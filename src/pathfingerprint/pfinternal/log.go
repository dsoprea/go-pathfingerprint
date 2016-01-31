package pfinternal

import (
    "os"

    log "gopkg.in/inconshreveable/log15.v2"
    "github.com/mattn/go-colorable"
)

const (
    LogPackageVersion = 1
)

var showDebug bool = false

func SetDebugLogging () {
    showDebug = true
}

func NewLogger(context string) log.Logger {
    l := log.New("context", context)

    return l
}

func ConfigureRootLogger () {
    sh := log.StreamHandler(colorable.NewColorableStdout(), log.TerminalFormat())

    logLevel := log.LvlInfo

    if showDebug == true {
        logLevel = log.LvlDebug
    } else {
        _, found := os.LookupEnv("DEBUG")
        if found == true {
            logLevel = log.LvlDebug
        }
    }

    fh := log.LvlFilterHandler(logLevel, sh)
    cfh := log.CallerFileHandler(fh)
    log.Root().SetHandler(cfh)
}
