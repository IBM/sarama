//go:build !functional

package sarama

import (
	"flag"
	"io"
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if f := flag.Lookup("test.v"); f != nil && f.Value.String() == "true" {
		Logger = log.New(os.Stderr, "[DEBUG] ", log.Lmicroseconds|log.Ltime)
	}
	os.Exit(m.Run())
}

// redirectLogger sends Logger output to w for the rest of the test.
// It prefers (*log.Logger).SetOutput so leftover goroutines from other tests
// (for example safeAsyncClose) do not race on a write to the Logger variable.
func redirectLogger(t *testing.T, w io.Writer) {
	t.Helper()
	std, ok := Logger.(*log.Logger)
	if !ok {
		orig := Logger
		Logger = log.New(w, "", 0)
		t.Cleanup(func() { Logger = orig })
		return
	}
	prev := std.Writer()
	std.SetOutput(w)
	t.Cleanup(func() { std.SetOutput(prev) })
}
