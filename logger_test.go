//go:build !functional

package sarama

import "testing"

// testLogger implements the StdLogger interface and records the text in the
// logs of the given T passed from Test functions.
// and records the text in the error log.
//
// nolint:unused
type testLogger struct {
	t *testing.T
}

// nolint:unused
func (l *testLogger) Print(v ...interface{}) {
	if l.t != nil {
		l.t.Helper()
		l.t.Log(v...)
	}
}

// nolint:unused
func (l *testLogger) Printf(format string, v ...interface{}) {
	if l.t != nil {
		l.t.Helper()
		l.t.Logf(format, v...)
	}
}

// nolint:unused
func (l *testLogger) Println(v ...interface{}) {
	if l.t != nil {
		l.t.Helper()
		l.t.Log(v...)
	}
}
