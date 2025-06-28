package sarama

type apiVersionRange struct {
	minVersion int16
	maxVersion int16
}

type apiVersionMap map[int16]*apiVersionRange
