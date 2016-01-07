package pfinternal

import (
    "fmt"
    "errors"

    log "gopkg.in/inconshreveable/log15.v2"
)

const (
    ShowDebugLogging = false
)

type Logger struct {
    log *log.Logger
}

func NewLogger() *Logger {
    l := log.New()
    return &Logger {log: &l}
}

func (self *Logger) Debug (message string, ctx ...interface{}) {
// TODO(dustin): Finish figuring-out how to set the log-level to hide these.

    if ShowDebugLogging == true {
        (*self.log).Debug(message, ctx...)
    }
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
