//go:build !functional

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
	if !V1_0_0_0.IsAtLeast(V0_9_0_0) {
		t.Error("! 1.0.0.0 >= 0.9.0.0")
	}
	if V0_9_0_0.IsAtLeast(V1_0_0_0) {
		t.Error("0.9.0.0 >= 1.0.0.0")
	}
}

func TestVersionParsing(t *testing.T) {
	validVersions := []string{
		"0.8.2.0",
		"0.8.2.1",
		"0.8.2.2",
		"0.9.0.0",
		"0.9.0.1",
		"0.10.0.0",
		"0.10.0.1",
		"0.10.1.0",
		"0.10.1.1",
		"0.10.2.0",
		"0.10.2.1",
		"0.10.2.2",
		"0.11.0.0",
		"0.11.0.1",
		"0.11.0.2",
		"1.0.0",
		"1.0.1",
		"1.0.2",
		"1.1.0",
		"1.1.1",
		"2.0.0",
		"2.0.1",
		"2.1.0",
		"2.1.1",
		"2.2.0",
		"2.2.1",
		"2.2.2",
		"2.3.0",
		"2.3.1",
		"2.4.0",
		"2.4.1",
		"2.5.0",
		"2.5.1",
		"2.6.0",
		"2.6.1",
		"2.6.2",
		"2.6.3",
		"2.7.0",
		"2.7.1",
		"2.7.2",
		"2.8.0",
		"2.8.1",
		"3.0.0",
		"3.0.1",
		"3.1.0",
		"3.1.1",
		"3.2.0",
	}
	for _, s := range validVersions {
		v, err := ParseKafkaVersion(s)
		if err != nil {
			t.Errorf("could not parse valid version %s: %s", s, err)
		}
		if v.String() != s {
			t.Errorf("version %s != %s", v.String(), s)
		}
	}

	invalidVersions := []string{"0.8.2-4", "0.8.20", "1.19.0.0", "1.0.x"}
	for _, s := range invalidVersions {
		if _, err := ParseKafkaVersion(s); err == nil {
			t.Errorf("invalid version %s parsed without error", s)
		}
	}
}
