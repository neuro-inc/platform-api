package log

import (
	"fmt"
	"log"
	"os"
)

var (
	stdLogFlags     = log.LstdFlags | log.Lshortfile | log.LUTC
	outputCallDepth = 2

	infoLogger  = log.New(os.Stderr, "INFO: ", stdLogFlags)
	errorLogger = log.New(os.Stderr, "ERROR: ", stdLogFlags)
	fatalLogger = log.New(os.Stderr, "FATAL: ", stdLogFlags)
)

// Infof prints info message according to a format
func Infof(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	infoLogger.Output(outputCallDepth, s)
}

// Errorf prints warning message according to a format
func Errorf(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	errorLogger.Output(outputCallDepth, s)
}

// Fatalf prints fatal message according to a format and exits program
func Fatalf(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	fatalLogger.Output(outputCallDepth, s)
	os.Exit(1)
}