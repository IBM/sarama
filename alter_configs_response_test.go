//go:build !functional

package sarama

import (
	"errors"
	"testing"
)

var (
	alterResponseEmpty = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 0, // no configs
	}

	alterResponsePopulated = []byte{
		0, 0, 0, 0, // throttle
		0, 0, 0, 1, // response
		0, 0, // errorcode
		0, 0, // string
		2, // topic
		0, 3, 'f', 'o', 'o',
	}
)

func TestAlterConfigsResponse(t *testing.T) {
	var response *AlterConfigsResponse

	response = &AlterConfigsResponse{
		Resources: []*AlterConfigsResourceResponse{},
	}
	testVersionDecodable(t, "empty", response, alterResponseEmpty, 0)
	if len(response.Resources) != 0 {
		t.Error("Expected no groups")
	}

	response = &AlterConfigsResponse{
		Resources: []*AlterConfigsResourceResponse{
			{
				ErrorCode: 0,
				ErrorMsg:  "",
				Type:      TopicResource,
				Name:      "foo",
			},
		},
	}
	testResponse(t, "response with error", response, alterResponsePopulated)
}

func TestAlterConfigError(t *testing.T) {
	// Assert that AlterConfigError satisfies error interface
	var err error = &AlterConfigError{
		Err: ErrInvalidConfig,
	}

	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected errors.Is to match ErrInvalidConfig")
	}

	got := err.Error()
	want := ErrInvalidConfig.Error()
	if got != want {
		t.Errorf("AlterConfigError.Error() = %v; want %v", got, want)
	}

	err = &AlterConfigError{
		Err:    ErrInvalidConfig,
		ErrMsg: "invalid config value",
	}
	got = err.Error()
	want = ErrInvalidConfig.Error() + " - invalid config value"
	if got != want {
		t.Errorf("AlterConfigError.Error() = %v; want %v", got, want)
	}

	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected errors.Is to match ErrInvalidConfig with ErrMsg set")
	}
}
