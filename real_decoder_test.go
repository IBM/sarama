package sarama

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{
			name:    "negative length other than null array",
			input:   []byte{0x80, 0x00, 0x00, 0x00},
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

func TestRealDecoderGetNullableInt32Array(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    []int32
		wantErr error
	}{
		{
			name:  "null array",
			input: []byte{0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name:  "empty array",
			input: []byte{0x00, 0x00, 0x00, 0x00},
			want:  []int32{},
		},
		{
			name:  "valid array",
			input: []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02},
			want:  []int32{1, 2},
		},
		{
			name:    "insufficient data for length",
			input:   []byte{0x00, 0x00, 0x00},
			wantErr: ErrInsufficientData,
		},
		{
			name:    "insufficient data for elements",
			input:   []byte{0x00, 0x00, 0x00, 0x02},
			wantErr: ErrInsufficientData,
		},
		{
			name:    "invalid length",
			input:   []byte{0x80, 0x00, 0x00, 0x00},
			wantErr: errInvalidArrayLength,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := &realDecoder{raw: tt.input}
			got, err := rd.getNullableInt32Array()
			require.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRealDecoderGetInt32Array(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    []int32
		wantErr error
	}{
		{
			name:    "null array",
			input:   []byte{0xFF, 0xFF, 0xFF, 0xFF},
			wantErr: errInvalidArrayLength,
		},
		{
			name:  "empty array",
			input: []byte{0x00, 0x00, 0x00, 0x00},
			want:  []int32{},
		},
		{
			name:  "valid array",
			input: []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02},
			want:  []int32{1, 2},
		},
		{
			name:    "insufficient data for length",
			input:   []byte{0x00, 0x00, 0x00},
			wantErr: ErrInsufficientData,
		},
		{
			name:    "insufficient data for elements",
			input:   []byte{0x00, 0x00, 0x00, 0x02},
			wantErr: ErrInsufficientData,
		},
		{
			name:    "invalid length",
			input:   []byte{0x80, 0x00, 0x00, 0x00},
			wantErr: errInvalidArrayLength,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := &realDecoder{raw: tt.input}
			got, err := rd.getInt32Array()
			require.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRealFlexibleDecoderGetNullableInt32Array(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    []int32
		wantErr error
	}{
		{
			name:  "null array",
			input: []byte{0x00},
		},
		{
			name:  "empty array",
			input: []byte{0x01},
			want:  []int32{},
		},
		{
			name:  "valid array",
			input: []byte{0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02},
			want:  []int32{1, 2},
		},
		{
			name:    "insufficient data",
			input:   []byte{0x03},
			wantErr: ErrInsufficientData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := &realFlexibleDecoder{&realDecoder{raw: tt.input}}
			got, err := rd.getNullableInt32Array()
			require.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRealFlexibleDecoderGetInt32Array(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    []int32
		wantErr error
	}{
		{
			name:    "null array",
			input:   []byte{0x00},
			wantErr: errInvalidArrayLength,
		},
		{
			name:  "empty array",
			input: []byte{0x01},
			want:  []int32{},
		},
		{
			name:    "valid array",
			input:   []byte{0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02},
			want:    []int32{1, 2},
			wantErr: nil,
		},
		{
			name:    "insufficient data",
			input:   []byte{0x03},
			wantErr: ErrInsufficientData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := &realFlexibleDecoder{&realDecoder{raw: tt.input}}
			got, err := rd.getInt32Array()
			require.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, tt.want, got)
		})
	}
	t.Run("returns insufficient data for compact length exceeding int64", func(t *testing.T) {
		var input [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(input[:], 1<<63)

		rd := &realFlexibleDecoder{&realDecoder{raw: input[:n]}}
		array, err := rd.getInt32Array()

		require.ErrorIs(t, err, ErrInsufficientData)
		assert.Nil(t, array)
	})
}
