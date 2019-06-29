package sarama

import (
	"errors"
	"fmt"
	"gopkg.in/jcmturner/gokrb5.v7/krberror"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
)

func ExampleBroker() {
	broker := NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		panic(err)
	}

	request := MetadataRequest{Topics: []string{"myTopic"}}
	response, err := broker.GetMetadata(&request)
	if err != nil {
		_ = broker.Close()
		panic(err)
	}

	fmt.Println("There are", len(response.Topics), "topics active in the cluster.")

	if err = broker.Close(); err != nil {
		panic(err)
	}
}

type mockEncoder struct {
	bytes []byte
}

func (m mockEncoder) encode(pe packetEncoder) error {
	return pe.putRawBytes(m.bytes)
}

type brokerMetrics struct {
	bytesRead    int
	bytesWritten int
}

func TestBrokerAccessors(t *testing.T) {
	broker := NewBroker("abc:123")

	if broker.ID() != -1 {
		t.Error("New broker didn't have an ID of -1.")
	}

	if broker.Addr() != "abc:123" {
		t.Error("New broker didn't have the correct address")
	}

	if broker.Rack() != "" {
		t.Error("New broker didn't have an unknown rack.")
	}

	broker.id = 34
	if broker.ID() != 34 {
		t.Error("Manually setting broker ID did not take effect.")
	}

	rack := "dc1"
	broker.rack = &rack
	if broker.Rack() != rack {
		t.Error("Manually setting broker rack did not take effect.")
	}
}

func TestSimpleBrokerCommunication(t *testing.T) {
	for _, tt := range brokerTestTable {
		Logger.Printf("Testing broker communication for %s", tt.name)
		mb := NewMockBroker(t, 0)
		mb.Returns(&mockEncoder{tt.response})
		pendingNotify := make(chan brokerMetrics)
		// Register a callback to be notified about successful requests
		mb.SetNotifier(func(bytesRead, bytesWritten int) {
			pendingNotify <- brokerMetrics{bytesRead, bytesWritten}
		})
		broker := NewBroker(mb.Addr())
		// Set the broker id in order to validate local broker metrics
		broker.id = 0
		conf := NewConfig()
		conf.Version = tt.version
		err := broker.Open(conf)
		if err != nil {
			t.Fatal(err)
		}
		tt.runner(t, broker)
		// Wait up to 500 ms for the remote broker to process the request and
		// notify us about the metrics
		timeout := 500 * time.Millisecond
		select {
		case mockBrokerMetrics := <-pendingNotify:
			validateBrokerMetrics(t, broker, mockBrokerMetrics)
		case <-time.After(timeout):
			t.Errorf("No request received for: %s after waiting for %v", tt.name, timeout)
		}
		mb.Close()
		err = broker.Close()
		if err != nil {
			t.Error(err)
		}
	}

}

var ErrTokenFailure = errors.New("Failure generating token")

type TokenProvider struct {
	accessToken *AccessToken
	err         error
}

func (t *TokenProvider) Token() (*AccessToken, error) {
	return t.accessToken, t.err
}

func newTokenProvider(token *AccessToken, err error) *TokenProvider {
	return &TokenProvider{
		accessToken: token,
		err:         err,
	}
}

func TestSASLOAuthBearer(t *testing.T) {

	testTable := []struct {
		name             string
		mockAuthErr      KError // Mock and expect error returned from SaslAuthenticateRequest
		mockHandshakeErr KError // Mock and expect error returned from SaslHandshakeRequest
		expectClientErr  bool   // Expect an internal client-side error
		tokProvider      *TokenProvider
	}{
		{
			name:             "SASL/OAUTHBEARER OK server response",
			mockAuthErr:      ErrNoError,
			mockHandshakeErr: ErrNoError,
			tokProvider:      newTokenProvider(&AccessToken{Token: "access-token-123"}, nil),
		},
		{
			name:             "SASL/OAUTHBEARER authentication failure response",
			mockAuthErr:      ErrSASLAuthenticationFailed,
			mockHandshakeErr: ErrNoError,
			tokProvider:      newTokenProvider(&AccessToken{Token: "access-token-123"}, nil),
		},
		{
			name:             "SASL/OAUTHBEARER handshake failure response",
			mockAuthErr:      ErrNoError,
			mockHandshakeErr: ErrSASLAuthenticationFailed,
			tokProvider:      newTokenProvider(&AccessToken{Token: "access-token-123"}, nil),
		},
		{
			name:             "SASL/OAUTHBEARER token generation error",
			mockAuthErr:      ErrNoError,
			mockHandshakeErr: ErrNoError,
			expectClientErr:  true,
			tokProvider:      newTokenProvider(&AccessToken{Token: "access-token-123"}, ErrTokenFailure),
		},
		{
			name:             "SASL/OAUTHBEARER invalid extension",
			mockAuthErr:      ErrNoError,
			mockHandshakeErr: ErrNoError,
			expectClientErr:  true,
			tokProvider: newTokenProvider(&AccessToken{
				Token:      "access-token-123",
				Extensions: map[string]string{"auth": "auth-value"},
			}, nil),
		},
	}

	for i, test := range testTable {

		// mockBroker mocks underlying network logic and broker responses
		mockBroker := NewMockBroker(t, 0)

		mockSASLAuthResponse := NewMockSaslAuthenticateResponse(t).SetAuthBytes([]byte("response_payload"))
		if test.mockAuthErr != ErrNoError {
			mockSASLAuthResponse = mockSASLAuthResponse.SetError(test.mockAuthErr)
		}

		mockSASLHandshakeResponse := NewMockSaslHandshakeResponse(t).SetEnabledMechanisms([]string{SASLTypeOAuth})
		if test.mockHandshakeErr != ErrNoError {
			mockSASLHandshakeResponse = mockSASLHandshakeResponse.SetError(test.mockHandshakeErr)
		}

		mockBroker.SetHandlerByMap(map[string]MockResponse{
			"SaslAuthenticateRequest": mockSASLAuthResponse,
			"SaslHandshakeRequest":    mockSASLHandshakeResponse,
		})

		// broker executes SASL requests against mockBroker
		broker := NewBroker(mockBroker.Addr())
		broker.requestRate = metrics.NilMeter{}
		broker.outgoingByteRate = metrics.NilMeter{}
		broker.incomingByteRate = metrics.NilMeter{}
		broker.requestSize = metrics.NilHistogram{}
		broker.responseSize = metrics.NilHistogram{}
		broker.responseRate = metrics.NilMeter{}
		broker.requestLatency = metrics.NilHistogram{}

		conf := NewConfig()
		conf.Net.SASL.Mechanism = SASLTypeOAuth
		conf.Net.SASL.TokenProvider = test.tokProvider

		broker.conf = conf

		dialer := net.Dialer{
			Timeout:   conf.Net.DialTimeout,
			KeepAlive: conf.Net.KeepAlive,
			LocalAddr: conf.Net.LocalAddr,
		}

		conn, err := dialer.Dial("tcp", mockBroker.listener.Addr().String())

		if err != nil {
			t.Fatal(err)
		}

		broker.conn = conn

		err = broker.authenticateViaSASL()

		if test.mockAuthErr != ErrNoError {
			if test.mockAuthErr != err {
				t.Errorf("[%d]:[%s] Expected %s auth error, got %s\n", i, test.name, test.mockAuthErr, err)
			}
		} else if test.mockHandshakeErr != ErrNoError {
			if test.mockHandshakeErr != err {
				t.Errorf("[%d]:[%s] Expected %s handshake error, got %s\n", i, test.name, test.mockHandshakeErr, err)
			}
		} else if test.expectClientErr && err == nil {
			t.Errorf("[%d]:[%s] Expected a client error and got none\n", i, test.name)
		} else if !test.expectClientErr && err != nil {
			t.Errorf("[%d]:[%s] Unexpected error, got %s\n", i, test.name, err)
		}

		mockBroker.Close()
	}
}

// A mock scram client.
type MockSCRAMClient struct {
	done bool
}

func (m *MockSCRAMClient) Begin(userName, password, authzID string) (err error) {
	return nil
}

func (m *MockSCRAMClient) Step(challenge string) (response string, err error) {
	if challenge == "" {
		return "ping", nil
	}
	if challenge == "pong" {
		m.done = true
		return "", nil
	}
	return "", errors.New("failed to authenticate :(")
}

func (m *MockSCRAMClient) Done() bool {
	return m.done
}

var _ SCRAMClient = &MockSCRAMClient{}

func TestSASLSCRAMSHAXXX(t *testing.T) {
	testTable := []struct {
		name               string
		mockHandshakeErr   KError
		mockSASLAuthErr    KError
		expectClientErr    bool
		scramClient        *MockSCRAMClient
		scramChallengeResp string
	}{
		{
			name:               "SASL/SCRAMSHAXXX successfull authentication",
			mockHandshakeErr:   ErrNoError,
			scramClient:        &MockSCRAMClient{},
			scramChallengeResp: "pong",
		},
		{
			name:               "SASL/SCRAMSHAXXX SCRAM client step error client",
			mockHandshakeErr:   ErrNoError,
			mockSASLAuthErr:    ErrNoError,
			scramClient:        &MockSCRAMClient{},
			scramChallengeResp: "gong",
			expectClientErr:    true,
		},
		{
			name:               "SASL/SCRAMSHAXXX server authentication error",
			mockHandshakeErr:   ErrNoError,
			mockSASLAuthErr:    ErrSASLAuthenticationFailed,
			scramClient:        &MockSCRAMClient{},
			scramChallengeResp: "pong",
		},
		{
			name:               "SASL/SCRAMSHAXXX unsupported SCRAM mechanism",
			mockHandshakeErr:   ErrUnsupportedSASLMechanism,
			mockSASLAuthErr:    ErrNoError,
			scramClient:        &MockSCRAMClient{},
			scramChallengeResp: "pong",
		},
	}

	for i, test := range testTable {

		// mockBroker mocks underlying network logic and broker responses
		mockBroker := NewMockBroker(t, 0)
		broker := NewBroker(mockBroker.Addr())
		// broker executes SASL requests against mockBroker
		broker.requestRate = metrics.NilMeter{}
		broker.outgoingByteRate = metrics.NilMeter{}
		broker.incomingByteRate = metrics.NilMeter{}
		broker.requestSize = metrics.NilHistogram{}
		broker.responseSize = metrics.NilHistogram{}
		broker.responseRate = metrics.NilMeter{}
		broker.requestLatency = metrics.NilHistogram{}

		mockSASLAuthResponse := NewMockSaslAuthenticateResponse(t).SetAuthBytes([]byte(test.scramChallengeResp))
		mockSASLHandshakeResponse := NewMockSaslHandshakeResponse(t).SetEnabledMechanisms([]string{SASLTypeSCRAMSHA256, SASLTypeSCRAMSHA512})

		if test.mockSASLAuthErr != ErrNoError {
			mockSASLAuthResponse = mockSASLAuthResponse.SetError(test.mockSASLAuthErr)
		}
		if test.mockHandshakeErr != ErrNoError {
			mockSASLHandshakeResponse = mockSASLHandshakeResponse.SetError(test.mockHandshakeErr)
		}

		mockBroker.SetHandlerByMap(map[string]MockResponse{
			"SaslAuthenticateRequest": mockSASLAuthResponse,
			"SaslHandshakeRequest":    mockSASLHandshakeResponse,
		})

		conf := NewConfig()
		conf.Net.SASL.Mechanism = SASLTypeSCRAMSHA512
		conf.Net.SASL.SCRAMClientGeneratorFunc = func() SCRAMClient { return test.scramClient }

		broker.conf = conf
		dialer := net.Dialer{
			Timeout:   conf.Net.DialTimeout,
			KeepAlive: conf.Net.KeepAlive,
			LocalAddr: conf.Net.LocalAddr,
		}

		conn, err := dialer.Dial("tcp", mockBroker.listener.Addr().String())

		if err != nil {
			t.Fatal(err)
		}

		broker.conn = conn

		err = broker.authenticateViaSASL()

		if test.mockSASLAuthErr != ErrNoError {
			if test.mockSASLAuthErr != err {
				t.Errorf("[%d]:[%s] Expected %s SASL authentication error, got %s\n", i, test.name, test.mockHandshakeErr, err)
			}
		} else if test.mockHandshakeErr != ErrNoError {
			if test.mockHandshakeErr != err {
				t.Errorf("[%d]:[%s] Expected %s handshake error, got %s\n", i, test.name, test.mockHandshakeErr, err)
			}
		} else if test.expectClientErr && err == nil {
			t.Errorf("[%d]:[%s] Expected a client error and got none\n", i, test.name)
		} else if !test.expectClientErr && err != nil {
			t.Errorf("[%d]:[%s] Unexpected error, got %s\n", i, test.name, err)
		}

		mockBroker.Close()
	}
}

func TestSASLPlainAuth(t *testing.T) {

	testTable := []struct {
		name             string
		mockAuthErr      KError // Mock and expect error returned from SaslAuthenticateRequest
		mockHandshakeErr KError // Mock and expect error returned from SaslHandshakeRequest
		expectClientErr  bool   // Expect an internal client-side error
	}{
		{
			name:             "SASL Plain OK server response",
			mockAuthErr:      ErrNoError,
			mockHandshakeErr: ErrNoError,
		},
		{
			name:             "SASL Plain authentication failure response",
			mockAuthErr:      ErrSASLAuthenticationFailed,
			mockHandshakeErr: ErrNoError,
		},
		{
			name:             "SASL Plain handshake failure response",
			mockAuthErr:      ErrNoError,
			mockHandshakeErr: ErrSASLAuthenticationFailed,
		},
	}

	for i, test := range testTable {

		// mockBroker mocks underlying network logic and broker responses
		mockBroker := NewMockBroker(t, 0)

		mockSASLAuthResponse := NewMockSaslAuthenticateResponse(t).
			SetAuthBytes([]byte(`response_payload`))

		if test.mockAuthErr != ErrNoError {
			mockSASLAuthResponse = mockSASLAuthResponse.SetError(test.mockAuthErr)
		}

		mockSASLHandshakeResponse := NewMockSaslHandshakeResponse(t).
			SetEnabledMechanisms([]string{SASLTypePlaintext})

		if test.mockHandshakeErr != ErrNoError {
			mockSASLHandshakeResponse = mockSASLHandshakeResponse.SetError(test.mockHandshakeErr)
		}

		mockBroker.SetHandlerByMap(map[string]MockResponse{
			"SaslAuthenticateRequest": mockSASLAuthResponse,
			"SaslHandshakeRequest":    mockSASLHandshakeResponse,
		})

		// broker executes SASL requests against mockBroker
		broker := NewBroker(mockBroker.Addr())
		broker.requestRate = metrics.NilMeter{}
		broker.outgoingByteRate = metrics.NilMeter{}
		broker.incomingByteRate = metrics.NilMeter{}
		broker.requestSize = metrics.NilHistogram{}
		broker.responseSize = metrics.NilHistogram{}
		broker.responseRate = metrics.NilMeter{}
		broker.requestLatency = metrics.NilHistogram{}

		conf := NewConfig()
		conf.Net.SASL.Mechanism = SASLTypePlaintext
		conf.Net.SASL.User = "token"
		conf.Net.SASL.Password = "password"
		conf.Net.SASL.Version = SASLHandshakeV1

		broker.conf = conf
		broker.conf.Version = V1_0_0_0
		dialer := net.Dialer{
			Timeout:   conf.Net.DialTimeout,
			KeepAlive: conf.Net.KeepAlive,
			LocalAddr: conf.Net.LocalAddr,
		}

		conn, err := dialer.Dial("tcp", mockBroker.listener.Addr().String())

		if err != nil {
			t.Fatal(err)
		}

		broker.conn = conn

		err = broker.authenticateViaSASL()

		if test.mockAuthErr != ErrNoError {
			if test.mockAuthErr != err {
				t.Errorf("[%d]:[%s] Expected %s auth error, got %s\n", i, test.name, test.mockAuthErr, err)
			}
		} else if test.mockHandshakeErr != ErrNoError {
			if test.mockHandshakeErr != err {
				t.Errorf("[%d]:[%s] Expected %s handshake error, got %s\n", i, test.name, test.mockHandshakeErr, err)
			}
		} else if test.expectClientErr && err == nil {
			t.Errorf("[%d]:[%s] Expected a client error and got none\n", i, test.name)
		} else if !test.expectClientErr && err != nil {
			t.Errorf("[%d]:[%s] Unexpected error, got %s\n", i, test.name, err)
		}

		mockBroker.Close()
	}
}

func TestGSSAPIKerberosAuth_Authorize(t *testing.T) {

	testTable := []struct {
		name               string
		error              error
		mockKerberosClient bool
		errorStage         string
		badResponse        bool
		badKeyChecksum     bool
	}{
		{
			name:               "Kerberos authentication success",
			error:              nil,
			mockKerberosClient: true,
		},
		{
			name: "Kerberos login fails",
			error: krberror.NewErrorf(krberror.KDCError, "KDC_Error: AS Exchange Error: "+
				"kerberos error response from KDC: KRB Error: (24) KDC_ERR_PREAUTH_FAILED Pre-authenti"+
				"cation information was invalid - PREAUTH_FAILED"),
			mockKerberosClient: true,
			errorStage:         "login",
		},
		{
			name: "Kerberos service ticket fails",
			error: krberror.NewErrorf(krberror.KDCError, "KDC_Error: AS Exchange Error: "+
				"kerberos error response from KDC: KRB Error: (24) KDC_ERR_PREAUTH_FAILED Pre-authenti"+
				"cation information was invalid - PREAUTH_FAILED"),
			mockKerberosClient: true,
			errorStage:         "service_ticket",
		},
		{
			name:  "Kerberos client creation fails",
			error: errors.New("configuration file could not be opened: krb5.conf open krb5.conf: no such file or directory"),
		},
		{
			name:               "Bad server response, unmarshall key error",
			error:              errors.New("bytes shorter than header length"),
			badResponse:        true,
			mockKerberosClient: true,
		},
		{
			name:               "Bad token checksum",
			error:              errors.New("checksum mismatch. Computed: 39feb88ac2459f2b77738493, Contained in token: ffffffffffffffff00000000"),
			badResponse:        false,
			badKeyChecksum:     true,
			mockKerberosClient: true,
		},
	}
	for i, test := range testTable {
		mockBroker := NewMockBroker(t, 0)
		// broker executes SASL requests against mockBroker

		mockBroker.SetGSSAPIHandler(func(bytes []byte) []byte {
			return nil
		})
		broker := NewBroker(mockBroker.Addr())
		broker.requestRate = metrics.NilMeter{}
		broker.outgoingByteRate = metrics.NilMeter{}
		broker.incomingByteRate = metrics.NilMeter{}
		broker.requestSize = metrics.NilHistogram{}
		broker.responseSize = metrics.NilHistogram{}
		broker.responseRate = metrics.NilMeter{}
		broker.requestLatency = metrics.NilHistogram{}
		conf := NewConfig()
		conf.Net.SASL.Mechanism = SASLTypeGSSAPI
		conf.Net.SASL.GSSAPI.ServiceName = "kafka"
		conf.Net.SASL.GSSAPI.KerberosConfigPath = "krb5.conf"
		conf.Net.SASL.GSSAPI.Realm = "EXAMPLE.COM"
		conf.Net.SASL.GSSAPI.Username = "kafka"
		conf.Net.SASL.GSSAPI.Password = "kafka"
		conf.Net.SASL.GSSAPI.KeyTabPath = "kafka.keytab"
		conf.Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH
		broker.conf = conf
		broker.conf.Version = V1_0_0_0
		dialer := net.Dialer{
			Timeout:   conf.Net.DialTimeout,
			KeepAlive: conf.Net.KeepAlive,
			LocalAddr: conf.Net.LocalAddr,
		}

		conn, err := dialer.Dial("tcp", mockBroker.listener.Addr().String())

		if err != nil {
			t.Fatal(err)
		}

		gssapiHandler := KafkaGSSAPIHandler{
			client:         &MockKerberosClient{},
			badResponse:    test.badResponse,
			badKeyChecksum: test.badKeyChecksum,
		}
		mockBroker.SetGSSAPIHandler(gssapiHandler.MockKafkaGSSAPI)
		broker.conn = conn
		if test.mockKerberosClient {
			broker.kerberosAuthenticator.NewKerberosClientFunc = func(config *GSSAPIConfig) (KerberosClient, error) {
				return &MockKerberosClient{
					mockError:  test.error,
					errorStage: test.errorStage,
				}, nil
			}
		} else {
			broker.kerberosAuthenticator.NewKerberosClientFunc = nil
		}

		err = broker.authenticateViaSASL()

		if err != nil && test.error != nil {
			if test.error.Error() != err.Error() {
				t.Errorf("[%d] Expected error:%s, got:%s.", i, test.error, err)
			}
		} else if (err == nil && test.error != nil) || (err != nil && test.error == nil) {
			t.Errorf("[%d] Expected error:%s, got:%s.", i, test.error, err)
		}

		mockBroker.Close()
	}

}

func TestBuildClientInitialResponse(t *testing.T) {

	testTable := []struct {
		name        string
		token       *AccessToken
		expected    []byte
		expectError bool
	}{
		{
			name: "Build SASL client initial response with two extensions",
			token: &AccessToken{
				Token: "the-token",
				Extensions: map[string]string{
					"x": "1",
					"y": "2",
				},
			},
			expected: []byte("n,,\x01auth=Bearer the-token\x01x=1\x01y=2\x01\x01"),
		},
		{
			name:     "Build SASL client initial response with no extensions",
			token:    &AccessToken{Token: "the-token"},
			expected: []byte("n,,\x01auth=Bearer the-token\x01\x01"),
		},
		{
			name: "Build SASL client initial response using reserved extension",
			token: &AccessToken{
				Token: "the-token",
				Extensions: map[string]string{
					"auth": "auth-value",
				},
			},
			expected:    []byte(""),
			expectError: true,
		},
	}

	for i, test := range testTable {

		actual, err := buildClientInitialResponse(test.token)

		if !reflect.DeepEqual(test.expected, actual) {
			t.Errorf("Expected %s, got %s\n", test.expected, actual)
		}
		if test.expectError && err == nil {
			t.Errorf("[%d]:[%s] Expected an error but did not get one", i, test.name)
		}
		if !test.expectError && err != nil {
			t.Errorf("[%d]:[%s] Expected no error but got %s\n", i, test.name, err)
		}
	}
}

// We're not testing encoding/decoding here, so most of the requests/responses will be empty for simplicity's sake
var brokerTestTable = []struct {
	version  KafkaVersion
	name     string
	response []byte
	runner   func(*testing.T, *Broker)
}{
	{V0_10_0_0,
		"MetadataRequest",
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := MetadataRequest{}
			response, err := broker.GetMetadata(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Metadata request got no response!")
			}
		}},

	{V0_10_0_0,
		"ConsumerMetadataRequest",
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 't', 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := ConsumerMetadataRequest{}
			response, err := broker.GetConsumerMetadata(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Consumer Metadata request got no response!")
			}
		}},

	{V0_10_0_0,
		"ProduceRequest (NoResponse)",
		[]byte{},
		func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = NoResponse
			response, err := broker.Produce(&request)
			if err != nil {
				t.Error(err)
			}
			if response != nil {
				t.Error("Produce request with NoResponse got a response!")
			}
		}},

	{V0_10_0_0,
		"ProduceRequest (WaitForLocal)",
		[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = WaitForLocal
			response, err := broker.Produce(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Produce request without NoResponse got no response!")
			}
		}},

	{V0_10_0_0,
		"FetchRequest",
		[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := FetchRequest{}
			response, err := broker.Fetch(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Fetch request got no response!")
			}
		}},

	{V0_10_0_0,
		"OffsetFetchRequest",
		[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := OffsetFetchRequest{}
			response, err := broker.FetchOffset(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("OffsetFetch request got no response!")
			}
		}},

	{V0_10_0_0,
		"OffsetCommitRequest",
		[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := OffsetCommitRequest{}
			response, err := broker.CommitOffset(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("OffsetCommit request got no response!")
			}
		}},

	{V0_10_0_0,
		"OffsetRequest",
		[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := OffsetRequest{}
			response, err := broker.GetAvailableOffsets(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Offset request got no response!")
			}
		}},

	{V0_10_0_0,
		"JoinGroupRequest",
		[]byte{0x00, 0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := JoinGroupRequest{}
			response, err := broker.JoinGroup(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("JoinGroup request got no response!")
			}
		}},

	{V0_10_0_0,
		"SyncGroupRequest",
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := SyncGroupRequest{}
			response, err := broker.SyncGroup(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("SyncGroup request got no response!")
			}
		}},

	{V0_10_0_0,
		"LeaveGroupRequest",
		[]byte{0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := LeaveGroupRequest{}
			response, err := broker.LeaveGroup(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("LeaveGroup request got no response!")
			}
		}},

	{V0_10_0_0,
		"HeartbeatRequest",
		[]byte{0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := HeartbeatRequest{}
			response, err := broker.Heartbeat(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Heartbeat request got no response!")
			}
		}},

	{V0_10_0_0,
		"ListGroupsRequest",
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := ListGroupsRequest{}
			response, err := broker.ListGroups(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("ListGroups request got no response!")
			}
		}},

	{V0_10_0_0,
		"DescribeGroupsRequest",
		[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := DescribeGroupsRequest{}
			response, err := broker.DescribeGroups(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("DescribeGroups request got no response!")
			}
		}},

	{V0_10_0_0,
		"ApiVersionsRequest",
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := ApiVersionsRequest{}
			response, err := broker.ApiVersions(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("ApiVersions request got no response!")
			}
		}},

	{V1_1_0_0,
		"DeleteGroupsRequest",
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := DeleteGroupsRequest{}
			response, err := broker.DeleteGroups(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("DeleteGroups request got no response!")
			}
		}},
}

func validateBrokerMetrics(t *testing.T, broker *Broker, mockBrokerMetrics brokerMetrics) {
	metricValidators := newMetricValidators()
	mockBrokerBytesRead := mockBrokerMetrics.bytesRead
	mockBrokerBytesWritten := mockBrokerMetrics.bytesWritten

	// Check that the number of bytes sent corresponds to what the mock broker received
	metricValidators.registerForAllBrokers(broker, countMeterValidator("incoming-byte-rate", mockBrokerBytesWritten))
	if mockBrokerBytesWritten == 0 {
		// This a ProduceRequest with NoResponse
		metricValidators.registerForAllBrokers(broker, countMeterValidator("response-rate", 0))
		metricValidators.registerForAllBrokers(broker, countHistogramValidator("response-size", 0))
		metricValidators.registerForAllBrokers(broker, minMaxHistogramValidator("response-size", 0, 0))
	} else {
		metricValidators.registerForAllBrokers(broker, countMeterValidator("response-rate", 1))
		metricValidators.registerForAllBrokers(broker, countHistogramValidator("response-size", 1))
		metricValidators.registerForAllBrokers(broker, minMaxHistogramValidator("response-size", mockBrokerBytesWritten, mockBrokerBytesWritten))
	}

	// Check that the number of bytes received corresponds to what the mock broker sent
	metricValidators.registerForAllBrokers(broker, countMeterValidator("outgoing-byte-rate", mockBrokerBytesRead))
	metricValidators.registerForAllBrokers(broker, countMeterValidator("request-rate", 1))
	metricValidators.registerForAllBrokers(broker, countHistogramValidator("request-size", 1))
	metricValidators.registerForAllBrokers(broker, minMaxHistogramValidator("request-size", mockBrokerBytesRead, mockBrokerBytesRead))

	// Run the validators
	metricValidators.run(t, broker.conf.MetricRegistry)
}
