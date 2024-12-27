//go:build !functional

package sarama

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rcrowley/go-metrics"
	assert "github.com/stretchr/testify/require"
)

// NewTestConfig returns a config meant to be used by tests.
// Due to inconsistencies with the request versions the clients send using the default Kafka version
// and the response versions our mocks use, we default to the minimum Kafka version in most tests
func NewTestConfig() *Config {
	config := NewConfig()
	config.Consumer.Retry.Backoff = 0
	config.Producer.Retry.Backoff = 0
	config.Version = MinVersion
	return config
}

func TestDefaultConfigValidates(t *testing.T) {
	config := NewTestConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
	if config.MetricRegistry == nil {
		t.Error("Expected non nil metrics.MetricRegistry, got nil")
	}
}

// TestInvalidClientIDValidated ensures that the ClientID field is checked
// when Version is set to anything less than 1_0_0_0, but otherwise accepted
func TestInvalidClientIDValidated(t *testing.T) {
	for _, version := range SupportedVersions {
		for _, clientID := range []string{"", "foo:bar", "foo|bar"} {
			config := NewTestConfig()
			config.ClientID = clientID
			config.Version = version
			err := config.Validate()
			if config.Version.IsAtLeast(V1_0_0_0) {
				assert.NoError(t, err)
				continue
			}
			var target ConfigurationError
			assert.ErrorAs(t, err, &target)
			assert.ErrorContains(t, err, fmt.Sprintf("ClientID value %q is not valid for Kafka versions before 1.0.0", clientID))
		}
	}
}

type DummyTokenProvider struct{}

func (t *DummyTokenProvider) Token() (*AccessToken, error) {
	return &AccessToken{Token: "access-token-string"}, nil
}

func TestNetConfigValidates(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*Config) // resorting to using a function as a param because of internal composite structs
		err  string
	}{
		{
			"OpenRequests",
			func(cfg *Config) {
				cfg.Net.MaxOpenRequests = 0
			},
			"Net.MaxOpenRequests must be > 0",
		},
		{
			"DialTimeout",
			func(cfg *Config) {
				cfg.Net.DialTimeout = 0
			},
			"Net.DialTimeout must be > 0",
		},
		{
			"ReadTimeout",
			func(cfg *Config) {
				cfg.Net.ReadTimeout = 0
			},
			"Net.ReadTimeout must be > 0",
		},
		{
			"WriteTimeout",
			func(cfg *Config) {
				cfg.Net.WriteTimeout = 0
			},
			"Net.WriteTimeout must be > 0",
		},
		{
			"SASL.User",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.User = ""
			},
			"Net.SASL.User must not be empty when SASL is enabled",
		},
		{
			"SASL.Password",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.User = "user"
				cfg.Net.SASL.Password = ""
			},
			"Net.SASL.Password must not be empty when SASL is enabled",
		},
		{
			"SASL.Mechanism - Invalid mechanism type",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.Mechanism = "AnIncorrectSASLMechanism"
				cfg.Net.SASL.TokenProvider = &DummyTokenProvider{}
			},
			"The SASL mechanism configuration is invalid. Possible values are `OAUTHBEARER`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` and `GSSAPI`",
		},
		{
			"SASL.Mechanism.OAUTHBEARER - Missing token provider",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.Mechanism = SASLTypeOAuth
				cfg.Net.SASL.TokenProvider = nil
			},
			"An AccessTokenProvider instance must be provided to Net.SASL.TokenProvider",
		},
		{
			"SASL.Mechanism SCRAM-SHA-256 - Missing SCRAM client",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.Mechanism = SASLTypeSCRAMSHA256
				cfg.Net.SASL.SCRAMClientGeneratorFunc = nil
				cfg.Net.SASL.User = "user"
				cfg.Net.SASL.Password = "strong_password"
			},
			"A SCRAMClientGeneratorFunc function must be provided to Net.SASL.SCRAMClientGeneratorFunc",
		},
		{
			"SASL.Mechanism SCRAM-SHA-512 - Missing SCRAM client",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.Mechanism = SASLTypeSCRAMSHA512
				cfg.Net.SASL.SCRAMClientGeneratorFunc = nil
				cfg.Net.SASL.User = "user"
				cfg.Net.SASL.Password = "strong_password"
			},
			"A SCRAMClientGeneratorFunc function must be provided to Net.SASL.SCRAMClientGeneratorFunc",
		},
		{
			"SASL.Mechanism GSSAPI (Kerberos) - Using User/Password, Missing password field",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.Mechanism = SASLTypeGSSAPI
				cfg.Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH
				cfg.Net.SASL.GSSAPI.Username = "sarama"
				cfg.Net.SASL.GSSAPI.ServiceName = "kafka"
				cfg.Net.SASL.GSSAPI.Realm = "kafka"
				cfg.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
			},
			"Net.SASL.GSSAPI.Password must not be empty when GSS-API " +
				"mechanism is used and Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH",
		},
		{
			"SASL.Mechanism GSSAPI (Kerberos) - Using User/Password, Missing KeyTabPath field",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.Mechanism = SASLTypeGSSAPI
				cfg.Net.SASL.GSSAPI.AuthType = KRB5_KEYTAB_AUTH
				cfg.Net.SASL.GSSAPI.Username = "sarama"
				cfg.Net.SASL.GSSAPI.ServiceName = "kafka"
				cfg.Net.SASL.GSSAPI.Realm = "kafka"
				cfg.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
			},
			"Net.SASL.GSSAPI.KeyTabPath must not be empty when GSS-API mechanism is used" +
				" and Net.SASL.GSSAPI.AuthType = KRB5_KEYTAB_AUTH",
		},
		{
			"SASL.Mechanism GSSAPI (Kerberos) - Missing username",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.Mechanism = SASLTypeGSSAPI
				cfg.Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH
				cfg.Net.SASL.GSSAPI.Password = "sarama"
				cfg.Net.SASL.GSSAPI.ServiceName = "kafka"
				cfg.Net.SASL.GSSAPI.Realm = "kafka"
				cfg.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
			},
			"Net.SASL.GSSAPI.Username must not be empty when GSS-API mechanism is used",
		},
		{
			"SASL.Mechanism GSSAPI (Kerberos) - Missing ServiceName",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.Mechanism = SASLTypeGSSAPI
				cfg.Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH
				cfg.Net.SASL.GSSAPI.Username = "sarama"
				cfg.Net.SASL.GSSAPI.Password = "sarama"
				cfg.Net.SASL.GSSAPI.Realm = "kafka"
				cfg.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
			},
			"Net.SASL.GSSAPI.ServiceName must not be empty when GSS-API mechanism is used",
		},
		{
			"SASL.Mechanism GSSAPI (Kerberos) - Missing AuthType",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.GSSAPI.ServiceName = "kafka"
				cfg.Net.SASL.Mechanism = SASLTypeGSSAPI
				cfg.Net.SASL.GSSAPI.Username = "sarama"
				cfg.Net.SASL.GSSAPI.Password = "sarama"
				cfg.Net.SASL.GSSAPI.Realm = "kafka"
				cfg.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
			},
			"Net.SASL.GSSAPI.AuthType is invalid. Possible values are KRB5_USER_AUTH, KRB5_KEYTAB_AUTH, and KRB5_CCACHE_AUTH",
		},
		{
			"SASL.Mechanism GSSAPI (Kerberos) - Missing KerberosConfigPath",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.GSSAPI.ServiceName = "kafka"
				cfg.Net.SASL.Mechanism = SASLTypeGSSAPI
				cfg.Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH
				cfg.Net.SASL.GSSAPI.Username = "sarama"
				cfg.Net.SASL.GSSAPI.Password = "sarama"
				cfg.Net.SASL.GSSAPI.Realm = "kafka"
			},
			"Net.SASL.GSSAPI.KerberosConfigPath must not be empty when GSS-API mechanism is used",
		},
		{
			"SASL.Mechanism GSSAPI (Kerberos) - Missing Realm",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.GSSAPI.ServiceName = "kafka"
				cfg.Net.SASL.Mechanism = SASLTypeGSSAPI
				cfg.Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH
				cfg.Net.SASL.GSSAPI.Username = "sarama"
				cfg.Net.SASL.GSSAPI.Password = "sarama"
				cfg.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
			},
			"Net.SASL.GSSAPI.Realm must not be empty when GSS-API mechanism is used",
		},
		{
			"SASL.Mechanism GSSAPI (Kerberos) - Using Credentials Cache, Missing CCachePath field",
			func(cfg *Config) {
				cfg.Net.SASL.Enable = true
				cfg.Net.SASL.GSSAPI.ServiceName = "kafka"
				cfg.Net.SASL.Mechanism = SASLTypeGSSAPI
				cfg.Net.SASL.GSSAPI.AuthType = KRB5_CCACHE_AUTH
				cfg.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
			},
			"Net.SASL.GSSAPI.CCachePath must not be empty when GSS-API mechanism is used" +
				" and Net.SASL.GSSAPI.AuthType = KRB5_CCACHE_AUTH",
		},
	}

	for i, test := range tests {
		c := NewTestConfig()
		test.cfg(c)
		err := c.Validate()
		var target ConfigurationError
		if !errors.As(err, &target) || string(target) != test.err {
			t.Errorf("[%d]:[%s] Expected %s, Got %s\n", i, test.name, test.err, err)
		}
	}
}

func TestMetadataConfigValidates(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*Config) // resorting to using a function as a param because of internal composite structs
		err  string
	}{
		{
			"Retry.Max",
			func(cfg *Config) {
				cfg.Metadata.Retry.Max = -1
			},
			"Metadata.Retry.Max must be >= 0",
		},
		{
			"Retry.Backoff",
			func(cfg *Config) {
				cfg.Metadata.Retry.Backoff = -1
			},
			"Metadata.Retry.Backoff must be >= 0",
		},
		{
			"RefreshFrequency",
			func(cfg *Config) {
				cfg.Metadata.RefreshFrequency = -1
			},
			"Metadata.RefreshFrequency must be >= 0",
		},
	}

	for i, test := range tests {
		c := NewTestConfig()
		test.cfg(c)
		err := c.Validate()
		var target ConfigurationError
		if !errors.As(err, &target) || string(target) != test.err {
			t.Errorf("[%d]:[%s] Expected %s, Got %s\n", i, test.name, test.err, err)
		}
	}
}

func TestAdminConfigValidates(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*Config) // resorting to using a function as a param because of internal composite structs
		err  string
	}{
		{
			"Timeout",
			func(cfg *Config) {
				cfg.Admin.Timeout = 0
			},
			"Admin.Timeout must be > 0",
		},
	}

	for i, test := range tests {
		c := NewTestConfig()
		test.cfg(c)
		err := c.Validate()
		var target ConfigurationError
		if !errors.As(err, &target) || string(target) != test.err {
			t.Errorf("[%d]:[%s] Expected %s, Got %s\n", i, test.name, test.err, err)
		}
	}
}

func TestProducerConfigValidates(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*Config) // resorting to using a function as a param because of internal composite structs
		err  string
	}{
		{
			"MaxMessageBytes",
			func(cfg *Config) {
				cfg.Producer.MaxMessageBytes = 0
			},
			"Producer.MaxMessageBytes must be > 0",
		},
		{
			"RequiredAcks",
			func(cfg *Config) {
				cfg.Producer.RequiredAcks = -2
			},
			"Producer.RequiredAcks must be >= -1",
		},
		{
			"Timeout",
			func(cfg *Config) {
				cfg.Producer.Timeout = 0
			},
			"Producer.Timeout must be > 0",
		},
		{
			"Partitioner",
			func(cfg *Config) {
				cfg.Producer.Partitioner = nil
			},
			"Producer.Partitioner must not be nil",
		},
		{
			"Flush.Bytes",
			func(cfg *Config) {
				cfg.Producer.Flush.Bytes = -1
			},
			"Producer.Flush.Bytes must be >= 0",
		},
		{
			"Flush.Messages",
			func(cfg *Config) {
				cfg.Producer.Flush.Messages = -1
			},
			"Producer.Flush.Messages must be >= 0",
		},
		{
			"Flush.Frequency",
			func(cfg *Config) {
				cfg.Producer.Flush.Frequency = -1
			},
			"Producer.Flush.Frequency must be >= 0",
		},
		{
			"Flush.MaxMessages",
			func(cfg *Config) {
				cfg.Producer.Flush.MaxMessages = -1
			},
			"Producer.Flush.MaxMessages must be >= 0",
		},
		{
			"Flush.MaxMessages with Producer.Flush.Messages",
			func(cfg *Config) {
				cfg.Producer.Flush.MaxMessages = 1
				cfg.Producer.Flush.Messages = 2
			},
			"Producer.Flush.MaxMessages must be >= Producer.Flush.Messages when set",
		},
		{
			"Flush.Retry.Max",
			func(cfg *Config) {
				cfg.Producer.Retry.Max = -1
			},
			"Producer.Retry.Max must be >= 0",
		},
		{
			"Flush.Retry.Backoff",
			func(cfg *Config) {
				cfg.Producer.Retry.Backoff = -1
			},
			"Producer.Retry.Backoff must be >= 0",
		},
		{
			"Idempotent Version",
			func(cfg *Config) {
				cfg.Producer.Idempotent = true
				cfg.Version = V0_10_0_0
			},
			"Idempotent producer requires Version >= V0_11_0_0",
		},
		{
			"Idempotent with Producer.Retry.Max",
			func(cfg *Config) {
				cfg.Version = V0_11_0_0
				cfg.Producer.Idempotent = true
				cfg.Producer.Retry.Max = 0
			},
			"Idempotent producer requires Producer.Retry.Max >= 1",
		},
		{
			"Idempotent with Producer.RequiredAcks",
			func(cfg *Config) {
				cfg.Version = V0_11_0_0
				cfg.Producer.Idempotent = true
			},
			"Idempotent producer requires Producer.RequiredAcks to be WaitForAll",
		},
		{
			"Idempotent with Net.MaxOpenRequests",
			func(cfg *Config) {
				cfg.Version = V0_11_0_0
				cfg.Producer.Idempotent = true
				cfg.Producer.RequiredAcks = WaitForAll
			},
			"Idempotent producer requires Net.MaxOpenRequests to be 1",
		},
	}

	for i, test := range tests {
		c := NewTestConfig()
		test.cfg(c)
		err := c.Validate()
		var target ConfigurationError
		if !errors.As(err, &target) || string(target) != test.err {
			t.Errorf("[%d]:[%s] Expected %s, Got %s\n", i, test.name, test.err, err)
		}
	}
}

func TestConsumerConfigValidates(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*Config)
		err  string
	}{
		{
			"ReadCommitted Version",
			func(cfg *Config) {
				cfg.Version = V0_10_0_0
				cfg.Consumer.IsolationLevel = ReadCommitted
			},
			"ReadCommitted requires Version >= V0_11_0_0",
		},
		{
			"Incorrect isolation level",
			func(cfg *Config) {
				cfg.Version = V0_11_0_0
				cfg.Consumer.IsolationLevel = IsolationLevel(42)
			},
			"Consumer.IsolationLevel must be ReadUncommitted or ReadCommitted",
		},
	}

	for i, test := range tests {
		c := NewTestConfig()
		test.cfg(c)
		err := c.Validate()
		var target ConfigurationError
		if !errors.As(err, &target) || string(target) != test.err {
			t.Errorf("[%d]:[%s] Expected %s, Got %s\n", i, test.name, test.err, err)
		}
	}
}

func TestLZ4ConfigValidation(t *testing.T) {
	config := NewTestConfig()
	config.Producer.Compression = CompressionLZ4
	err := config.Validate()
	var target ConfigurationError
	if !errors.As(err, &target) || string(target) != "lz4 compression requires Version >= V0_10_0_0" {
		t.Error("Expected invalid lz4/kafka version error, got ", err)
	}
	config.Version = V0_10_0_0
	if err := config.Validate(); err != nil {
		t.Error("Expected lz4 to work, got ", err)
	}
}

func TestZstdConfigValidation(t *testing.T) {
	config := NewTestConfig()
	config.Producer.Compression = CompressionZSTD
	err := config.Validate()
	var target ConfigurationError
	if !errors.As(err, &target) || string(target) != "zstd compression requires Version >= V2_1_0_0" {
		t.Error("Expected invalid zstd/kafka version error, got ", err)
	}
	config.Version = V2_1_0_0
	if err := config.Validate(); err != nil {
		t.Error("Expected zstd to work, got ", err)
	}
}

func TestValidGroupInstanceId(t *testing.T) {
	tests := []struct {
		grouptInstanceId string
		shouldHaveErr    bool
	}{
		{"groupInstanceId1", false},
		{"", true},
		{".", true},
		{"..", true},
		{strings.Repeat("a", 250), true},
		{"group_InstanceId.1", false},
		{"group-InstanceId1", false},
		{"group#InstanceId1", true},
	}
	for _, testcase := range tests {
		err := validateGroupInstanceId(testcase.grouptInstanceId)
		if !testcase.shouldHaveErr {
			if err != nil {
				t.Errorf("Expected validGroupInstanceId %s to pass, got error %v", testcase.grouptInstanceId, err)
			}
		} else {
			if err == nil {
				t.Errorf("Expected validGroupInstanceId %s to be error, got nil", testcase.grouptInstanceId)
			}
			var target ConfigurationError
			if !errors.As(err, &target) {
				t.Errorf("Excepted err to be ConfigurationError, got %v", err)
			}
		}
	}
}

func TestGroupInstanceIdAndVersionValidation(t *testing.T) {
	config := NewTestConfig()
	config.Consumer.Group.InstanceId = "groupInstanceId1"
	if err := config.Validate(); !strings.Contains(err.Error(), "Consumer.Group.InstanceId need Version >= 2.3") {
		t.Error("Expected invalid group instance error, got ", err)
	}
	config.Version = V2_3_0_0
	if err := config.Validate(); err != nil {
		t.Error("Expected group instance to work, got ", err)
	}
}

func TestConsumerGroupStrategyCompatibility(t *testing.T) {
	config := NewTestConfig()
	config.Consumer.Group.Rebalance.Strategy = NewBalanceStrategySticky()
	if err := config.Validate(); err != nil {
		t.Error("Expected passing config validation, got ", err)
	}
}

// This example shows how to integrate with an existing registry as well as publishing metrics
// on the standard output
func ExampleConfig_metrics() {
	// Our application registry
	appMetricRegistry := metrics.NewRegistry()
	appGauge := metrics.GetOrRegisterGauge("m1", appMetricRegistry)
	appGauge.Update(1)

	config := NewTestConfig()
	// Use a prefix registry instead of the default local one
	config.MetricRegistry = metrics.NewPrefixedChildRegistry(appMetricRegistry, "sarama.")

	// Simulate a metric created by sarama without starting a broker
	saramaGauge := metrics.GetOrRegisterGauge("m2", config.MetricRegistry)
	saramaGauge.Update(2)

	metrics.WriteOnce(appMetricRegistry, os.Stdout)
	// Output:
	// gauge m1
	//   value:               1
	// gauge sarama.m2
	//   value:               2
}
