//go:build !functional

package sarama

import (
	"errors"
	"reflect"
	"testing"
)

var (
	joinGroupResponseV0_NoError = []byte{
		0x00, 0x00, // No error
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 8, 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l', // Protocol name chosen
		0, 3, 'f', 'o', 'o', // Leader ID
		0, 3, 'b', 'a', 'r', // Member ID
		0, 0, 0, 0, // No member info
	}

	joinGroupResponseV0_WithError = []byte{
		0, 23, // Error: inconsistent group protocol
		0x00, 0x00, 0x00, 0x00, // Generation ID
		0, 0, // Protocol name chosen
		0, 0, // Leader ID
		0, 0, // Member ID
		0, 0, 0, 0, // No member info
	}

	joinGroupResponseV0_Leader = []byte{
		0x00, 0x00, // No error
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 8, 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l', // Protocol name chosen
		0, 3, 'f', 'o', 'o', // Leader ID
		0, 3, 'f', 'o', 'o', // Member ID == Leader ID
		0, 0, 0, 1, // 1 member
		0, 3, 'f', 'o', 'o', // Member ID
		0, 0, 0, 3, 0x01, 0x02, 0x03, // Member metadata
	}

	joinGroupResponseV1 = []byte{
		0x00, 0x00, // No error
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 8, 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l', // Protocol name chosen
		0, 3, 'f', 'o', 'o', // Leader ID
		0, 3, 'b', 'a', 'r', // Member ID
		0, 0, 0, 0, // No member info
	}

	joinGroupResponseV2 = []byte{
		0, 0, 0, 100,
		0x00, 0x00, // No error
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 8, 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l', // Protocol name chosen
		0, 3, 'f', 'o', 'o', // Leader ID
		0, 3, 'b', 'a', 'r', // Member ID
		0, 0, 0, 0, // No member info
	}
)

func TestJoinGroupResponseV0(t *testing.T) {
	var response *JoinGroupResponse

	response = new(JoinGroupResponse)
	testVersionDecodable(t, "no error", response, joinGroupResponseV0_NoError, 0)
	if !errors.Is(response.Err, ErrNoError) {
		t.Error("Decoding Err failed: no error expected but found", response.Err)
	}
	if response.GenerationId != 66051 {
		t.Error("Decoding GenerationId failed, found:", response.GenerationId)
	}
	if response.LeaderId != "foo" {
		t.Error("Decoding LeaderId failed, found:", response.LeaderId)
	}
	if response.MemberId != "bar" {
		t.Error("Decoding MemberId failed, found:", response.MemberId)
	}
	if len(response.Members) != 0 {
		t.Error("Decoding Members failed, found:", response.Members)
	}

	response = new(JoinGroupResponse)
	testVersionDecodable(t, "with error", response, joinGroupResponseV0_WithError, 0)
	if !errors.Is(response.Err, ErrInconsistentGroupProtocol) {
		t.Error("Decoding Err failed: ErrInconsistentGroupProtocol expected but found", response.Err)
	}
	if response.GenerationId != 0 {
		t.Error("Decoding GenerationId failed, found:", response.GenerationId)
	}
	if response.LeaderId != "" {
		t.Error("Decoding LeaderId failed, found:", response.LeaderId)
	}
	if response.MemberId != "" {
		t.Error("Decoding MemberId failed, found:", response.MemberId)
	}
	if len(response.Members) != 0 {
		t.Error("Decoding Members failed, found:", response.Members)
	}

	response = new(JoinGroupResponse)
	testVersionDecodable(t, "with error", response, joinGroupResponseV0_Leader, 0)
	if !errors.Is(response.Err, ErrNoError) {
		t.Error("Decoding Err failed: ErrNoError expected but found", response.Err)
	}
	if response.GenerationId != 66051 {
		t.Error("Decoding GenerationId failed, found:", response.GenerationId)
	}
	if response.LeaderId != "foo" {
		t.Error("Decoding LeaderId failed, found:", response.LeaderId)
	}
	if response.MemberId != "foo" {
		t.Error("Decoding MemberId failed, found:", response.MemberId)
	}
	if len(response.Members) != 1 {
		t.Error("Decoding Members failed, found:", response.Members)
	}
	if response.Members[0].MemberId != "foo" {
		t.Error("Decoding MemberId failed, found:", response.Members[0].MemberId)
	}
	if !reflect.DeepEqual(response.Members[0].Metadata, []byte{0x01, 0x02, 0x03}) {
		t.Error("Decoding foo member failed, found:", response.Members[0].Metadata)
	}
}

func TestJoinGroupResponseV1(t *testing.T) {
	response := new(JoinGroupResponse)
	testVersionDecodable(t, "no error", response, joinGroupResponseV1, 1)
	if !errors.Is(response.Err, ErrNoError) {
		t.Error("Decoding Err failed: no error expected but found", response.Err)
	}
	if response.GenerationId != 66051 {
		t.Error("Decoding GenerationId failed, found:", response.GenerationId)
	}
	if response.GroupProtocol != "protocol" {
		t.Error("Decoding GroupProtocol failed, found:", response.GroupProtocol)
	}
	if response.LeaderId != "foo" {
		t.Error("Decoding LeaderId failed, found:", response.LeaderId)
	}
	if response.MemberId != "bar" {
		t.Error("Decoding MemberId failed, found:", response.MemberId)
	}
	if response.Version != 1 {
		t.Error("Decoding Version failed, found:", response.Version)
	}
	if len(response.Members) != 0 {
		t.Error("Decoding Members failed, found:", response.Members)
	}
}

func TestJoinGroupResponseV2(t *testing.T) {
	response := new(JoinGroupResponse)
	testVersionDecodable(t, "no error", response, joinGroupResponseV2, 2)
	if response.ThrottleTime != 100 {
		t.Error("Decoding ThrottleTime failed, found:", response.ThrottleTime)
	}
	if !errors.Is(response.Err, ErrNoError) {
		t.Error("Decoding Err failed: no error expected but found", response.Err)
	}
	if response.GenerationId != 66051 {
		t.Error("Decoding GenerationId failed, found:", response.GenerationId)
	}
	if response.GroupProtocol != "protocol" {
		t.Error("Decoding GroupProtocol failed, found:", response.GroupProtocol)
	}
	if response.LeaderId != "foo" {
		t.Error("Decoding LeaderId failed, found:", response.LeaderId)
	}
	if response.MemberId != "bar" {
		t.Error("Decoding MemberId failed, found:", response.MemberId)
	}
	if response.Version != 2 {
		t.Error("Decoding Version failed, found:", response.Version)
	}
	if len(response.Members) != 0 {
		t.Error("Decoding Members failed, found:", response.Members)
	}
}

var (
	joinGroupResponseV5 = []byte{
		0, 0, 0, 100, // ThrottleTimeMs
		0x00, 0x00, // No error
		0x00, 0x01, 0x02, 0x03, // Generation ID
		0, 8, 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l', // Protocol name chosen
		0, 3, 'f', 'o', 'o', // Leader ID
		0, 3, 'b', 'a', 'r', // Member ID
		0, 0, 0, 1, // One member info
		0, 3, 'm', 'i', 'd', // memberId
		0, 3, 'g', 'i', 'd', // GroupInstanceId
		0, 0, 0, 3, 1, 2, 3, // Metadata
	}
)

func TestJoinGroupResponse3plus(t *testing.T) {
	groupInstanceId := "gid"
	tests := []struct {
		CaseName     string
		Version      int16
		MessageBytes []byte
		Message      *JoinGroupResponse
	}{
		{
			"v5",
			5,
			joinGroupResponseV5,
			&JoinGroupResponse{
				Version:       5,
				ThrottleTime:  100,
				Err:           ErrNoError,
				GenerationId:  0x00010203,
				GroupProtocol: "protocol",
				LeaderId:      "foo",
				MemberId:      "bar",
				Members: []GroupMember{
					{"mid", &groupInstanceId, []byte{1, 2, 3}},
				},
			},
		},
	}
	for _, c := range tests {
		response := new(JoinGroupResponse)
		testVersionDecodable(t, c.CaseName, response, c.MessageBytes, c.Version)
		if !reflect.DeepEqual(c.Message, response) {
			t.Errorf("case %s decode failed, expected:%+v got %+v", c.CaseName, c.Message, response)
		}
		testEncodable(t, c.CaseName, c.Message, c.MessageBytes)
	}
}
