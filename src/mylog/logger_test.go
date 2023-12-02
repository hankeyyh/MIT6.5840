package mylog

import "testing"

func TestLogger(t *testing.T) {
	SetLogpath("./test.log")
	Info("a", "b", "c")
	Infof("%s: %d", "age", 15)
}