package mylog

import (
	"fmt"
	"log"
	"os"
	"io"
)

const (
	_callDepth = 3
)

var (
	defaultLogger *DefaultLogger = NewDefaultLogger(os.Stdout, log.Ldate | log.Lmicroseconds | log.Lshortfile)
)

type DefaultLogger struct {
	*log.Logger
}

func NewDefaultLogger(out io.Writer, flag int) *DefaultLogger {
	dl := new(DefaultLogger)
	dl.Logger = log.New(out, "", flag)

	return dl
}

func (dl *DefaultLogger) SetLogpath(filename string) error {
	ofile, err := os.OpenFile(filename, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	dl.SetOutput(ofile)
	return nil
}

func (dl *DefaultLogger) Debug(v ...interface{}) {
	dl.Output(_callDepth, "[DEBUG]" + fmt.Sprint(v...))
}

func (dl *DefaultLogger) Debugf(format string, v ...interface{}) {
	dl.Output(_callDepth, "[DEBUG]" + fmt.Sprintf(format, v...))
}

func (dl *DefaultLogger) Info(v ...interface{}) {
	dl.Output(_callDepth, "[INFO]" + fmt.Sprint(v...))
}

func (dl *DefaultLogger) Infof(format string, v ...interface{}) {
	dl.Output(_callDepth, "[INFO]" + fmt.Sprintf(format, v...))
}

func (dl *DefaultLogger) Warn(v ...interface{}) {
	dl.Output(_callDepth, "[WARN]" + fmt.Sprint(v...))
}

func (dl *DefaultLogger) Warnf(format string, v ...interface{}) {
	dl.Output(_callDepth, "[WARN]" + fmt.Sprintf(format, v...))
}

func (dl *DefaultLogger) Error(v ...interface{}) {
	dl.Output(_callDepth, "[ERROR]" + fmt.Sprint(v...))
}

func (dl *DefaultLogger) Errorf(format string, v ...interface{}) {
	dl.Output(_callDepth, "[ERROR]" + fmt.Sprintf(format, v...))
}

func (dl *DefaultLogger) Fatal(v ...interface{}) {
	dl.Output(_callDepth, "[FATAL]" + fmt.Sprint(v...))
	os.Exit(1)
}

func (dl *DefaultLogger) Fatalf(format string, v ...interface{}) {
	dl.Output(_callDepth, "[FATAL]" + fmt.Sprintf(format, v...))
	os.Exit(1)
}
