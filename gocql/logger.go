package gocql

import "log"

type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type stdLoggerWrapper struct {
	StdLogger
}

var Logger StdLogger = log.Default()
