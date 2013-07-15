package kafka

// make []int32 sortable so we can sort partition numbers

type int32Slice []int32

func (slice int32Slice) Len() int {
	return len(slice)
}

func (slice int32Slice) Less(i, j int) bool {
	return slice[i] < slice[j]
}

func (slice int32Slice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// make strings encodable for convenience so they can be used as keys
// and/or values in kafka messages

type encodableString string

func (s encodableString) Encode() ([]byte, error) {
	return []byte(s), nil
}
