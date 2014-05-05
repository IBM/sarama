package cluster

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Create GUIDs
var GUID = &gUIDFactory{pid: os.Getpid(), inc: 0xffffffff, mtx: new(sync.Mutex)}

type gUIDFactory struct {
	hostname string
	pid      int
	inc      uint32
	mtx      *sync.Mutex
}

// Init gUIDFactory's hostname
func init() {
	GUID.hostname, _ = os.Hostname()
	if GUID.hostname == "" {
		GUID.hostname = "localhost"
	}
}

// Create a
func (g *gUIDFactory) New(prefix string) string {
	return g.NewAt(prefix, time.Now())
}

// Create a new GUID for a certain time
func (g *gUIDFactory) NewAt(prefix string, at time.Time) string {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	g.inc++
	return fmt.Sprintf("%s-%s-%d-%d-%d", prefix, g.hostname, g.pid, at.Unix(), g.inc)
}
