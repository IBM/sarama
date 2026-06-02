//go:build !functional

package sarama

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecompress(t *testing.T) {
	codecs := []CompressionCodec{
		CompressionGZIP,
		CompressionSnappy,
		CompressionLZ4,
		CompressionZSTD,
	}

	const limit = 64 * 1024

	withLimit := func(t *testing.T, v int32) {
		t.Helper()
		old := MaxDecompressedBatchSize
		MaxDecompressedBatchSize = v
		t.Cleanup(func() { MaxDecompressedBatchSize = old })
	}

	t.Run("rejects a compression bomb under the wire limit", func(t *testing.T) {
		for _, cc := range codecs {
			t.Run(cc.String(), func(t *testing.T) {
				withLimit(t, limit)

				// highly compressible input that decompresses well past the cap
				payload := make([]byte, limit*8)
				compressed, err := compress(cc, CompressionLevelDefault, payload)
				require.NoError(t, err)
				require.Less(t, len(compressed), int(MaxResponseSize))

				out, err := decompress(cc, compressed)
				require.ErrorIs(t, err, ErrDecompressedBatchTooLarge)
				require.Empty(t, out)
			})
		}
	})

	t.Run("decodes a batch well under the cap", func(t *testing.T) {
		for _, cc := range codecs {
			t.Run(cc.String(), func(t *testing.T) {
				withLimit(t, limit)

				payload := bytes.Repeat([]byte("sarama"), 1000)
				require.Less(t, len(payload), limit)

				compressed, err := compress(cc, CompressionLevelDefault, payload)
				require.NoError(t, err)

				out, err := decompress(cc, compressed)
				require.NoError(t, err)
				require.Equal(t, payload, out)
			})
		}
	})

	t.Run("accepts exactly the cap and rejects one byte over", func(t *testing.T) {
		for _, cc := range codecs {
			t.Run(cc.String(), func(t *testing.T) {
				withLimit(t, limit)

				atLimit, err := compress(cc, CompressionLevelDefault, make([]byte, limit))
				require.NoError(t, err)
				out, err := decompress(cc, atLimit)
				require.NoError(t, err)
				require.Len(t, out, limit)

				over, err := compress(cc, CompressionLevelDefault, make([]byte, limit+1))
				require.NoError(t, err)
				_, err = decompress(cc, over)
				require.ErrorIs(t, err, ErrDecompressedBatchTooLarge)
			})
		}
	})

	t.Run("leaves decompression unbounded when unset", func(t *testing.T) {
		for _, cc := range codecs {
			t.Run(cc.String(), func(t *testing.T) {
				withLimit(t, 0)

				payload := make([]byte, limit*8)
				compressed, err := compress(cc, CompressionLevelDefault, payload)
				require.NoError(t, err)

				out, err := decompress(cc, compressed)
				require.NoError(t, err)
				require.Len(t, out, len(payload))
			})
		}
	})
}
