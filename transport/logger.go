package transport

import "log"

type Logger interface {
	Print(v ...any)
	Printf(format string, v ...any)
	Println(v ...any)
}

type DefaultLogger struct{}

func (n DefaultLogger) Print(_ ...any)            {}
func (n DefaultLogger) Printf(_ string, _ ...any) {}
func (n DefaultLogger) Println(_ ...any)          {}

type DebugLogger struct{}

func (n DebugLogger) Print(v ...any)                 { log.Print(v...) }
func (n DebugLogger) Printf(format string, v ...any) { log.Printf(format, v...) }
func (n DebugLogger) Println(v ...any)               { log.Println(v...) }
