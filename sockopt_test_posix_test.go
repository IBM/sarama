//go:build unix && !android && !illumos && !ios && !hurd

package sarama

func socketErrorProbeAvailable() bool {
	return true
}
