//go:build !functional

package sarama

import (
	"flag"
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
