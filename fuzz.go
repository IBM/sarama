// +build gofuzz

package sarama

import "bytes"

func Fuzz(data []byte) int {
	_, _, err := decodeRequest(bytes.NewReader(data))
	if err != nil {
		return 0
	}
	return 1
}
