package sarama

import "testing"

func TestVersionCompare(t *testing.T) {
	if V0_8_2_0.IsAtLeast(V0_8_2_1) {
		t.Error("0.8.2.0 >= 0.8.2.1")
	}
	if !V0_8_2_1.IsAtLeast(V0_8_2_0) {
		t.Error("! 0.8.2.1 >= 0.8.2.0")
	}
	if !V0_8_2_0.IsAtLeast(V0_8_2_0) {
		t.Error("! 0.8.2.0 >= 0.8.2.0")
	}
	if !V0_9_0_0.IsAtLeast(V0_8_2_1) {
		t.Error("! 0.9.0.0 >= 0.8.2.1")
	}
	if V0_8_2_1.IsAtLeast(V0_10_0_0) {
		t.Error("0.8.2.1 >= 0.10.0.0")
	}
}

func TestVersionValues(t *testing.T) {
	validNames := []string{"0.8.2.0", "0.8.2.1", "0.9.0.0", "0.10.2.0", "1.0.0"}
	for _, name := range validNames {
		if _, ok := KafkaVersionValue[name]; !ok {
			t.Errorf("valid version name %s missing in value map", name)
		}
	}

	invalidNames := []string{"0.8.2.4", "0.8.20.1", "0.19.0.0", "1.0.x"}
	for _, name := range invalidNames {
		if _, ok := KafkaVersionValue[name]; ok {
			t.Errorf("invalid version name %s found in value map", name)
		}
	}
}

func TestVersionNames(t *testing.T) {
	validValues := map[string]KafkaVersion{
		"V0_8_2_0":  V0_8_2_0,
		"V0_8_2_1":  V0_8_2_1,
		"V0_9_0_0":  V0_9_0_0,
		"V0_10_2_0": V0_10_2_0,
		"V1_0_0_0":  V1_0_0_0,
	}
	for s, value := range validValues {
		if _, ok := KafkaVersionName[value]; !ok {
			t.Errorf("kafka version %s missing in name map", s)
		}
		if value.String() == "" {
			t.Errorf("%s.String() == \"\"", s)
		}
	}
}
