//go:build !functional

package sarama

import (
	"errors"
	"testing"

	krbcfg "github.com/jcmturner/gokrb5/v8/config"
)

/*
 * Minimum requirement for client creation
 * we are not testing the client itself, we only test that the client is created
 * properly.
 *
 */

const (
	krb5cfg = `[libdefaults]
  default_realm = TEST.GOKRB5
  dns_lookup_realm = false
  dns_lookup_kdc = false
  ticket_lifetime = 24h
  forwardable = yes
  default_tkt_enctypes = aes256-cts-hmac-sha1-96
  default_tgs_enctypes = aes256-cts-hmac-sha1-96
  noaddresses = false
[realms]
 TEST.GOKRB5 = {
  kdc = 127.0.0.1:88
  admin_server = 127.0.0.1:749
  default_domain = test.gokrb5
 }
 RESDOM.GOKRB5 = {
  kdc = 10.80.88.88:188
  admin_server = 127.0.0.1:749
  default_domain = resdom.gokrb5
 }
  USER.GOKRB5 = {
  kdc = 192.168.88.100:88
  admin_server = 192.168.88.100:464
  default_domain = user.gokrb5
 }
  RES.GOKRB5 = {
  kdc = 192.168.88.101:88
  admin_server = 192.168.88.101:464
  default_domain = res.gokrb5
 }
[domain_realm]
 .test.gokrb5 = TEST.GOKRB5
 test.gokrb5 = TEST.GOKRB5
 .resdom.gokrb5 = RESDOM.GOKRB5
 resdom.gokrb5 = RESDOM.GOKRB5
  .user.gokrb5 = USER.GOKRB5
 user.gokrb5 = USER.GOKRB5
  .res.gokrb5 = RES.GOKRB5
 res.gokrb5 = RES.GOKRB5
`
)

func TestFaildToCreateKerberosConfig(t *testing.T) {
	expectedErr := errors.New("configuration file could not be opened: krb5.conf open krb5.conf: no such file or directory")
	clientConfig := NewTestConfig()
	clientConfig.Net.SASL.Mechanism = SASLTypeGSSAPI
	clientConfig.Net.SASL.Enable = true
	clientConfig.Net.SASL.GSSAPI.ServiceName = "kafka"
	clientConfig.Net.SASL.GSSAPI.Realm = "EXAMPLE.COM"
	clientConfig.Net.SASL.GSSAPI.Username = "client"
	clientConfig.Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH
	clientConfig.Net.SASL.GSSAPI.Password = "qwerty"
	clientConfig.Net.SASL.GSSAPI.KerberosConfigPath = "krb5.conf"
	_, err := NewKerberosClient(&clientConfig.Net.SASL.GSSAPI)
	// Expect to create client with password
	if err.Error() != expectedErr.Error() {
		t.Errorf("Expected error:%s, got:%s.", err, expectedErr)
	}
}

func TestCreateWithPassword(t *testing.T) {
	kerberosConfig, err := krbcfg.NewFromString(krb5cfg)
	if err != nil {
		t.Fatal(err)
	}
	expectedDoman := "EXAMPLE.COM"
	expectedCName := "client"

	clientConfig := NewTestConfig()
	clientConfig.Net.SASL.Mechanism = SASLTypeGSSAPI
	clientConfig.Net.SASL.Enable = true
	clientConfig.Net.SASL.GSSAPI.ServiceName = "kafka"
	clientConfig.Net.SASL.GSSAPI.Realm = "EXAMPLE.COM"
	clientConfig.Net.SASL.GSSAPI.Username = "client"
	clientConfig.Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH
	clientConfig.Net.SASL.GSSAPI.Password = "qwerty"
	clientConfig.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
	client, _ := createClient(&clientConfig.Net.SASL.GSSAPI, kerberosConfig)
	// Expect to create client with password
	if client == nil {
		t.Errorf("Expected client not nil")
	}
	if client.Domain() != expectedDoman {
		t.Errorf("Client domain: %s, got: %s", expectedDoman, client.Domain())
	}
	if client.CName().NameString[0] != expectedCName {
		t.Errorf("Client domain:%s, got: %s", expectedCName, client.CName().NameString[0])
	}
}

func TestCreateWithKeyTab(t *testing.T) {
	kerberosConfig, err := krbcfg.NewFromString(krb5cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Expect to try to create a client with keytab and fails with "o such file or directory" error
	expectedErr := errors.New("open nonexist.keytab: no such file or directory")
	clientConfig := NewTestConfig()
	clientConfig.Net.SASL.Mechanism = SASLTypeGSSAPI
	clientConfig.Net.SASL.Enable = true
	clientConfig.Net.SASL.GSSAPI.ServiceName = "kafka"
	clientConfig.Net.SASL.GSSAPI.Realm = "EXAMPLE.COM"
	clientConfig.Net.SASL.GSSAPI.Username = "client"
	clientConfig.Net.SASL.GSSAPI.AuthType = KRB5_KEYTAB_AUTH
	clientConfig.Net.SASL.GSSAPI.KeyTabPath = "nonexist.keytab"
	clientConfig.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
	_, err = createClient(&clientConfig.Net.SASL.GSSAPI, kerberosConfig)
	if err.Error() != expectedErr.Error() {
		t.Errorf("Expected error:%s, got:%s.", err, expectedErr)
	}
}

func TestCreateWithCredentialsCache(t *testing.T) {
	kerberosConfig, err := krbcfg.NewFromString(krb5cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Expect to try to create a client with a credentials cache and fails with "o such file or directory" error
	expectedErr := errors.New("open nonexist.ccache: no such file or directory")
	clientConfig := NewTestConfig()
	clientConfig.Net.SASL.Mechanism = SASLTypeGSSAPI
	clientConfig.Net.SASL.Enable = true
	clientConfig.Net.SASL.GSSAPI.ServiceName = "kafka"
	clientConfig.Net.SASL.GSSAPI.AuthType = KRB5_CCACHE_AUTH
	clientConfig.Net.SASL.GSSAPI.CCachePath = "nonexist.ccache"
	clientConfig.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
	_, err = createClient(&clientConfig.Net.SASL.GSSAPI, kerberosConfig)
	if err.Error() != expectedErr.Error() {
		t.Errorf("Expected error:%s, got:%s.", err, expectedErr)
	}
}

func TestCreateWithDisablePAFXFAST(t *testing.T) {
	kerberosConfig, err := krbcfg.NewFromString(krb5cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Expect to try to create a client with keytab and fails with "o such file or directory" error
	expectedErr := errors.New("open nonexist.keytab: no such file or directory")
	clientConfig := NewTestConfig()
	clientConfig.Net.SASL.Mechanism = SASLTypeGSSAPI
	clientConfig.Net.SASL.Enable = true
	clientConfig.Net.SASL.GSSAPI.ServiceName = "kafka"
	clientConfig.Net.SASL.GSSAPI.Realm = "EXAMPLE.COM"
	clientConfig.Net.SASL.GSSAPI.Username = "client"
	clientConfig.Net.SASL.GSSAPI.AuthType = KRB5_KEYTAB_AUTH
	clientConfig.Net.SASL.GSSAPI.KeyTabPath = "nonexist.keytab"
	clientConfig.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
	clientConfig.Net.SASL.GSSAPI.DisablePAFXFAST = true

	_, err = createClient(&clientConfig.Net.SASL.GSSAPI, kerberosConfig)
	if err.Error() != expectedErr.Error() {
		t.Errorf("Expected error:%s, got:%s.", err, expectedErr)
	}
}
