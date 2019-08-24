package sarama

import (
	"errors"
	krbcfg "gopkg.in/jcmturner/gokrb5.v7/config"
	"gopkg.in/jcmturner/gokrb5.v7/test/testdata"
	"testing"
)

/*
 * Minimum requirement for client creation
 * we are not testing the client itself, we only test that the client is created
 * properly.
 *
 */
const TEST_KRB5CONF = `
[libdefaults]
default_realm = EXAMPLE.COM

[realms]
EXAMPLE.COM = {
kdc = kerberos.example.com
admin_server = kerberos.example.com
}

[domain_realm]
.example.com = EXAMPLE.COM
example.com = EXAMPLE.COM
`

func TestFaildToCreateKerberosConfig(t *testing.T) {
	expectedErr := errors.New("configuration file could not be opened: krb5.conf open krb5.conf: no such file or directory")
	clientConfig := NewConfig()
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
	kerberosConfig, err := krbcfg.NewConfigFromString(testdata.TEST_KRB5CONF)
	if err != nil {
		t.Fatal(err)
	}
	expectedDoman := "EXAMPLE.COM"
	expectedCName := "client"

	clientConfig := NewConfig()
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
	kerberosConfig, err := krbcfg.NewConfigFromString(testdata.TEST_KRB5CONF)
	if err != nil {
		t.Fatal(err)
	}
	// Expect to try to create a client with keytab and fails with "o such file or directory" error
	expectedErr := errors.New("open nonexist.keytab: no such file or directory")
	clientConfig := NewConfig()
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
