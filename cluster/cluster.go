package cluster

// COMMON TYPES

// Partition information
type Partition struct {
	Id   int32
	Addr string // Leader address
}

// A sortable slice of Partition structs
type PartitionSlice []Partition

func (s PartitionSlice) Len() int { return len(s) }
func (s PartitionSlice) Less(i, j int) bool {
	if s[i].Addr < s[j].Addr {
		return true
	}
	return s[i].Id < s[j].Id
}
func (s PartitionSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Loggable interface
type Loggable interface {
	Printf(string, ...interface{})
}
