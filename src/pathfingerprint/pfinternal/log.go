package pfinternal

import (
    "fmt"
    "errors"
    "os"
    "strconv"

    log "gopkg.in/inconshreveable/log15.v2"
    "github.com/mattn/go-colorable"
)

type Logger struct {
    log *log.Logger
}

func NewLogger(context string) *Logger {
    l := log.New("context", context)

    return &Logger {log: &l}
}

func (self *Logger) ConfigureRootLogger () {
    screenHandler := log.StreamHandler(colorable.NewColorableStdout(), log.TerminalFormat())

    logLevel := log.LvlInfo

    value, found := os.LookupEnv("PF_DEBUG")
    if found == true {
        flag, err := strconv.ParseBool(value)
        if err != nil {
            fmt.Println("Debug value not valid (try '0' or '1').")
            os.Exit(99)
        } else if flag == true {
            logLevel = log.LvlDebug
        }
    }

    filterHandler := log.LvlFilterHandler(logLevel, screenHandler)
    log.Root().SetHandler(filterHandler)
}

func (self *Logger) Debug (message string, ctx ...interface{}) {
    (*self.log).Debug(message, ctx...)
}

func (self *Logger) Info (message string, ctx ...interface{}) {
    (*self.log).Info(message, ctx...)
}

func (self *Logger) Warn (message string, ctx ...interface{}) {
    (*self.log).Warn(message, ctx...)
}

func (self *Logger) Error (message string, ctx ...interface{}) {
    (*self.log).Error(message, ctx...)
}

func (self *Logger) Critical (message string, ctx ...interface{}) {
    (*self.log).Crit(message, ctx...)
}

// Log the error and then return the message combined with the error (so it can 
// be returned).
func (self *Logger) MergeAndLogError (currentError error, newMessage string, ctx ...interface{}) error {
    currentMessage := currentError.Error()
    combinedMessage := fmt.Sprintf("%s: %s", newMessage, currentMessage)

    tempCtx := append([]interface{}{"error", currentError}, ctx...)

    self.Error(newMessage, tempCtx...)

    mergedError := errors.New(combinedMessage)
    return mergedError
}

// Log a new error and then return an error object (so it can be returned).
func (self *Logger) LogError (message string, ctx ...interface{}) error {
    self.Error(message, ctx...)

    error := errors.New(message)
    return error
}

// Log and die only if err is not nil.
func (self *Logger) DieIf (err error, message string, ctx ...interface{}) {
    if err != nil {
        tempCtx := append([]interface{}{"error", err}, ctx...)

        self.Critical(message, tempCtx...)
        panic(err)
    }
}
