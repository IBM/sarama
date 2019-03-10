package sarama

type ControlRecordType int

const (
	ControlRecordAbort ControlRecordType = iota
	ControlRecordCommit
	ControlRecordUnknown
)

// Control records are returned as a record by fetchRequest
// However unlike "normal" records, they mean nothing application wise.
// They only serve internal logic for supporting transactions.
type ControlRecord struct {
	Version          int16
	CoordinatorEpoch int32
	Type             ControlRecordType
}
