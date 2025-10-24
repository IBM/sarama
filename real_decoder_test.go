package sarama

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestRealDecoder_getArrayLength(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantLen int
		wantErr error
	}{
		{
			name:    "null array (-1)",
			input:   []byte{0xFF, 0xFF, 0xFF, 0xFF},
			wantLen: -1,
			wantErr: nil,
		},
		{
			name:    "valid array length 64",
			input:   makeInput(64),
			wantLen: 64,
			wantErr: nil,
		},
		{
			name:    "valid array up to MaxResponseSize",
			input:   makeInput(int(MaxResponseSize)),
			wantLen: int(MaxResponseSize),
			wantErr: nil,
		},
		{
			name:    "insufficient data",
			input:   []byte{0x00, 0x00, 0x00}, // fewer than 4 bytes
			wantLen: -1,
			wantErr: ErrInsufficientData,
		},
		{
			name:    "length exceeds remaining",
			input:   []byte{0x00, 0x00, 0x00, 0x05, 0x00}, // length of 5, but only 1 byte remains
			wantLen: -1,
			wantErr: ErrInsufficientData,
		},
		{
			name:    "length exceeds MaxResponseSize",
			input:   makeInput(int(MaxResponseSize + 1)),
			wantLen: -1,
			wantErr: errInvalidArrayLength,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := &realDecoder{
				raw: tt.input,
			}
			gotLen, gotErr := rd.getArrayLength()
			if gotLen != tt.wantLen {
				t.Errorf("getArrayLength() gotLen = %v, want %v", gotLen, tt.wantLen)
			}
			if !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("getArrayLength() gotErr = %v, want %v", gotErr, tt.wantErr)
			}
		})
	}
}

func makeInput(length int) []byte {
	input := make([]byte, 4+length)
	binary.BigEndian.PutUint32(input, uint32(length)) // #nosec G115 - not going to exceed uint32
	return input
}
