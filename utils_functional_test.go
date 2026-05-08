//go:build functional

package sarama

// reduced set of protocol versions to matrix test
var fvtRangeVersions = []KafkaVersion{
	V0_8_2_2,
	V0_10_2_2,
	V1_0_2_0,
	V1_1_1_0,
	V2_0_1_0,
	V2_2_2_0,
	V2_4_1_0,
	V2_6_3_0,
	V2_8_2_0,
	V3_1_2_0,
	V3_3_2_0,
	V3_6_2_0,
}
