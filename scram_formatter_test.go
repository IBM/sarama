//go:build !functional

package sarama

import (
	"bytes"
	"testing"
)

/*
Following code can be used to validate saltedPassword implementation:

<pre>
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import java.nio.charset.StandardCharsets;

public class App {

    public static String bytesToHex(byte[] in) {
        final StringBuilder builder = new StringBuilder();
        for(byte b : in) {
            builder.append(String.format("0x%02x, ", b));
        }
        return builder.toString();
    }

	public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException {
	   int digestIterations = 4096;
	   String password = "hello";
	   byte[] salt = "world".getBytes(StandardCharsets.UTF_8);
	   byte[] saltedPassword = new ScramFormatter(ScramMechanism.SCRAM_SHA_256)
			   .saltedPassword(password, salt, digestIterations);
	   System.out.println(bytesToHex(saltedPassword));
	}
}
</pre>
*/

func TestScramSaltedPasswordSha512(t *testing.T) {
	password := []byte("hello")
	salt := []byte("world")

	formatter := scramFormatter{mechanism: SCRAM_MECHANISM_SHA_512}
	result, _ := formatter.saltedPassword(password, salt, 4096)

	// calculated using ScramFormatter (see comment above)
	expected := []byte{
		0x35, 0x0c, 0x77, 0x84, 0x8a, 0x63, 0x06, 0x92, 0x00,
		0x6e, 0xc6, 0x6a, 0x0c, 0x39, 0xeb, 0xb0, 0x00, 0xd3,
		0xf8, 0x8a, 0x94, 0xae, 0x7f, 0x8c, 0xcd, 0x1d, 0x92,
		0x52, 0x6c, 0x5b, 0x16, 0x15, 0x86, 0x3b, 0xde, 0xa1,
		0x6c, 0x12, 0x9a, 0x7b, 0x09, 0xed, 0x0e, 0x38, 0xf2,
		0x07, 0x4d, 0x2f, 0xe2, 0x9f, 0x0f, 0x41, 0xe1, 0xfb,
		0x00, 0xc1, 0xd3, 0xbd, 0xd3, 0xfd, 0x51, 0x0b, 0xa9,
		0x8f,
	}

	if !bytes.Equal(result, expected) {
		t.Errorf("saltedPassword SHA-512 failed, expected: %v, result: %v", expected, result)
	}
}

func TestScramSaltedPasswordSha256(t *testing.T) {
	password := []byte("hello")
	salt := []byte("world")

	formatter := scramFormatter{mechanism: SCRAM_MECHANISM_SHA_256}
	result, _ := formatter.saltedPassword(password, salt, 4096)

	// calculated using ScramFormatter (see comment above)
	expected := []byte{
		0xc1, 0x55, 0x53, 0x03, 0xda, 0x30, 0x9f, 0x6b, 0x7d,
		0x1e, 0x8f, 0xe4, 0x56, 0x36, 0xbf, 0xdd, 0xdc, 0x4b,
		0xf5, 0x64, 0x05, 0xe7, 0xe9, 0x4e, 0x9d, 0x15, 0xf0,
		0xe7, 0xb9, 0xcb, 0xd3, 0x80,
	}

	if !bytes.Equal(result, expected) {
		t.Errorf("saltedPassword SHA-256 failed, expected: %v, result: %v", expected, result)
	}
}
