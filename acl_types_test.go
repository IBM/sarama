//go:build !functional

package sarama

import (
	"testing"
)

func TestAclOperationTextMarshal(t *testing.T) {
	for i := AclOperationUnknown; i <= AclOperationTwoPhaseCommit; i++ {
		text, err := i.MarshalText()
		if err != nil {
			t.Errorf("couldn't marshal %d to text: %s", i, err)
		}
		var got AclOperation
		err = got.UnmarshalText(text)
		if err != nil {
			t.Errorf("couldn't unmarshal %s to acl operation: %s", text, err)
		}
		if got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}

func TestAclPermissionTypeTextMarshal(t *testing.T) {
	for i := AclPermissionUnknown; i <= AclPermissionAllow; i++ {
		text, err := i.MarshalText()
		if err != nil {
			t.Errorf("couldn't marshal %d to text: %s", i, err)
		}
		var got AclPermissionType
		err = got.UnmarshalText(text)
		if err != nil {
			t.Errorf("couldn't unmarshal %s to acl permission: %s", text, err)
		}
		if got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}

func TestAclResourceTypeTextMarshal(t *testing.T) {
	for i := AclResourceUnknown; i <= AclResourceUser; i++ {
		text, err := i.MarshalText()
		if err != nil {
			t.Errorf("couldn't marshal %d to text: %s", i, err)
		}
		var got AclResourceType
		err = got.UnmarshalText(text)
		if err != nil {
			t.Errorf("couldn't unmarshal %s to acl resource: %s", text, err)
		}
		if got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}

func TestAclResourcePatternTypeTextMarshal(t *testing.T) {
	for i := AclPatternUnknown; i <= AclPatternPrefixed; i++ {
		text, err := i.MarshalText()
		if err != nil {
			t.Errorf("couldn't marshal %d to text: %s", i, err)
		}
		var got AclResourcePatternType
		err = got.UnmarshalText(text)
		if err != nil {
			t.Errorf("couldn't unmarshal %s to acl resource pattern: %s", text, err)
		}
		if got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}

func TestAclTypesTextMarshalKIP373(t *testing.T) {
	// values added to the Java enums for delegation tokens (KIP-373) and
	// two-phase commit (KIP-939)
	for code, name := range map[AclOperation]string{13: "CreateTokens", 14: "DescribeTokens", 15: "TwoPhaseCommit"} {
		text, err := code.MarshalText()
		if err != nil || string(text) != name {
			t.Errorf("AclOperation(%d).MarshalText() = %q, %v; want %q", code, text, err, name)
		}
		var got AclOperation
		if err := got.UnmarshalText([]byte(name)); err != nil || got != code {
			t.Errorf("UnmarshalText(%q) = %d, %v; want %d", name, got, err, code)
		}
	}

	user := AclResourceType(7)
	text, err := user.MarshalText()
	if err != nil || string(text) != "User" {
		t.Errorf("AclResourceType(7).MarshalText() = %q, %v; want %q", text, err, "User")
	}
	var got AclResourceType
	if err := got.UnmarshalText([]byte("User")); err != nil || got != user {
		t.Errorf("UnmarshalText(%q) = %d, %v; want %d", "User", got, err, user)
	}
}
