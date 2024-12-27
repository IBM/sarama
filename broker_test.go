//go:build !functional

package sarama

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/jcmturner/gokrb5/v8/krberror"
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

func (m mockEncoder) headerVersion() int16 {
	return 0
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

type produceResponsePromise struct {
	c chan produceResOrError
}

type produceResOrError struct {
	res *ProduceResponse
	err error
}

func newProduceResponsePromise() produceResponsePromise {
	return produceResponsePromise{
		c: make(chan produceResOrError, 0),
	}
}

func (p produceResponsePromise) callback(res *ProduceResponse, err error) {
	if err != nil {
		p.c <- produceResOrError{
			err: err,
		}
		return
	}
	p.c <- produceResOrError{
		res: res,
	}
}

func (p produceResponsePromise) Get() (*ProduceResponse, error) {
	resOrError := <-p.c
	return resOrError.res, resOrError.err
}

func TestSimpleBrokerCommunication(t *testing.T) {
	for _, tt := range brokerTestTable {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
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
			conf := NewTestConfig()
			conf.ApiVersionsRequest = false
			conf.Version = tt.version
			err := broker.Open(conf)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := broker.Connected(); err != nil {
				t.Error(err)
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
		})
	}
}

func TestBrokerFailedRequest(t *testing.T) {
	for _, tt := range brokerFailedReqTestTable {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Testing broker communication for %s", tt.name)
			mb := NewMockBroker(t, 0)
			if !tt.stopBroker {
				mb.Returns(&mockEncoder{tt.response})
			}
			broker := NewBroker(mb.Addr())
			// Stop the broker before calling the runner to purposefully
			// make the request fail right away, the port will be closed
			// and should not be reused right away
			if tt.stopBroker {
				t.Log("Closing broker:", mb.Addr())
				mb.Close()
			}
			conf := NewTestConfig()
			conf.ApiVersionsRequest = false
			conf.Version = tt.version
			// Tune read timeout to speed up some test cases
			conf.Net.ReadTimeout = 1 * time.Second
			err := broker.Open(conf)
			if err != nil {
				t.Fatal(err)
			}
			tt.runner(t, broker)
			if !tt.stopBroker {
				mb.Close()
			}
			err = broker.Close()
			if err != nil {
				if tt.stopBroker && errors.Is(err, ErrNotConnected) {
					// We expect the broker to not close properly
					return
				}
				t.Error(err)
			}
		})
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
		name                      string
		authidentity              string
		mockSASLHandshakeResponse MockResponse // Mock SaslHandshakeRequest response from broker
		mockSASLAuthResponse      MockResponse // Mock SaslAuthenticateRequest response from broker
		expectClientErr           bool         // Expect an internal client-side error
		expectedBrokerError       KError       // Expected Kafka error returned by client
		tokProvider               *TokenProvider
	}{
		{
			name: "SASL/OAUTHBEARER OK server response",
			mockSASLHandshakeResponse: NewMockSaslHandshakeResponse(t).
				SetEnabledMechanisms([]string{SASLTypeOAuth}),
			mockSASLAuthResponse: NewMockSaslAuthenticateResponse(t),
			expectClientErr:      false,
			expectedBrokerError:  ErrNoError,
			tokProvider:          newTokenProvider(&AccessToken{Token: "access-token-123"}, nil),
		},
		{
			name: "SASL/OAUTHBEARER authentication failure response",
			mockSASLHandshakeResponse: NewMockSaslHandshakeResponse(t).
				SetEnabledMechanisms([]string{SASLTypeOAuth}),
			mockSASLAuthResponse: NewMockSequence(
				// First, the broker response with a challenge
				NewMockSaslAuthenticateResponse(t).
					SetAuthBytes([]byte(`{"status":"invalid_request1"}`)),
				// Next, the client terminates the token exchange. Finally, the
				// broker responds with an error message.
				NewMockSaslAuthenticateResponse(t).
					SetAuthBytes([]byte(`{"status":"invalid_request2"}`)).
					SetError(ErrSASLAuthenticationFailed),
			),
			expectClientErr:     true,
			expectedBrokerError: ErrSASLAuthenticationFailed,
			tokProvider:         newTokenProvider(&AccessToken{Token: "access-token-123"}, nil),
		},
		{
			name: "SASL/OAUTHBEARER handshake failure response",
			mockSASLHandshakeResponse: NewMockSaslHandshakeResponse(t).
				SetEnabledMechanisms([]string{SASLTypeOAuth}).
				SetError(ErrSASLAuthenticationFailed),
			mockSASLAuthResponse: NewMockSaslAuthenticateResponse(t),
			expectClientErr:      true,
			expectedBrokerError:  ErrSASLAuthenticationFailed,
			tokProvider:          newTokenProvider(&AccessToken{Token: "access-token-123"}, nil),
		},
		{
			name: "SASL/OAUTHBEARER token generation error",
			mockSASLHandshakeResponse: NewMockSaslHandshakeResponse(t).
				SetEnabledMechanisms([]string{SASLTypeOAuth}),
			mockSASLAuthResponse: NewMockSaslAuthenticateResponse(t),
			expectClientErr:      true,
			expectedBrokerError:  ErrNoError,
			tokProvider:          newTokenProvider(&AccessToken{Token: "access-token-123"}, ErrTokenFailure),
		},
		{
			name: "SASL/OAUTHBEARER invalid extension",
			mockSASLHandshakeResponse: NewMockSaslHandshakeResponse(t).
				SetEnabledMechanisms([]string{SASLTypeOAuth}),
			mockSASLAuthResponse: NewMockSaslAuthenticateResponse(t),
			expectClientErr:      true,
			expectedBrokerError:  ErrNoError,
			tokProvider: newTokenProvider(&AccessToken{
				Token:      "access-token-123",
				Extensions: map[string]string{"auth": "auth-value"},
			}, nil),
		},
	}

	for i, test := range testTable {
		test := test
		t.Run(test.name, func(t *testing.T) {
			// mockBroker mocks underlying network logic and broker responses
			mockBroker := NewMockBroker(t, 0)

			mockBroker.SetHandlerByMap(map[string]MockResponse{
				"SaslAuthenticateRequest": test.mockSASLAuthResponse,
				"SaslHandshakeRequest":    test.mockSASLHandshakeResponse,
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
			broker.requestsInFlight = metrics.NilCounter{}

			conf := NewTestConfig()
			conf.Net.SASL.Mechanism = SASLTypeOAuth
			conf.Net.SASL.TokenProvider = test.tokProvider
			conf.Net.SASL.Enable = true
			conf.Version = V1_0_0_0

			err := broker.Open(conf)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = broker.Close() })

			_, err = broker.Connected()
			if !errors.Is(test.expectedBrokerError, ErrNoError) {
				if !errors.Is(err, test.expectedBrokerError) {
					t.Errorf("[%d]:[%s] Expected %s auth error, got %s\n", i, test.name, test.expectedBrokerError, err)
				}
			} else if test.expectClientErr && err == nil {
				t.Errorf("[%d]:[%s] Expected a client error and got none\n", i, test.name)
			} else if !test.expectClientErr && err != nil {
				t.Errorf("[%d]:[%s] Unexpected error, got %s\n", i, test.name, err)
			}

			mockBroker.Close()
		})
	}
}

// A mock scram client.
type MockSCRAMClient struct {
	done bool
}

func (m *MockSCRAMClient) Begin(_, _, _ string) (err error) {
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
			name:               "SASL/SCRAMSHAXXX successful authentication",
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
		test := test
		t.Run(test.name, func(t *testing.T) {
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
			broker.requestsInFlight = metrics.NilCounter{}

			mockSASLAuthResponse := NewMockSaslAuthenticateResponse(t).SetAuthBytes([]byte(test.scramChallengeResp))
			mockSASLHandshakeResponse := NewMockSaslHandshakeResponse(t).SetEnabledMechanisms([]string{SASLTypeSCRAMSHA256, SASLTypeSCRAMSHA512})

			if !errors.Is(test.mockSASLAuthErr, ErrNoError) {
				mockSASLAuthResponse = mockSASLAuthResponse.SetError(test.mockSASLAuthErr)
			}
			if !errors.Is(test.mockHandshakeErr, ErrNoError) {
				mockSASLHandshakeResponse = mockSASLHandshakeResponse.SetError(test.mockHandshakeErr)
			}

			mockBroker.SetHandlerByMap(map[string]MockResponse{
				"SaslAuthenticateRequest": mockSASLAuthResponse,
				"SaslHandshakeRequest":    mockSASLHandshakeResponse,
			})

			conf := NewTestConfig()
			conf.Net.SASL.Mechanism = SASLTypeSCRAMSHA512
			conf.Net.SASL.Version = SASLHandshakeV1
			conf.Net.SASL.User = "user"
			conf.Net.SASL.Password = "pass"
			conf.Net.SASL.Enable = true
			conf.Net.SASL.SCRAMClientGeneratorFunc = func() SCRAMClient { return test.scramClient }
			conf.Version = V1_0_0_0

			err := broker.Open(conf)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = broker.Close() })

			_, err = broker.Connected()

			if !errors.Is(test.mockSASLAuthErr, ErrNoError) {
				if !errors.Is(err, test.mockSASLAuthErr) {
					t.Errorf("[%d]:[%s] Expected %s SASL authentication error, got %s\n", i, test.name, test.mockHandshakeErr, err)
				}
			} else if !errors.Is(test.mockHandshakeErr, ErrNoError) {
				if !errors.Is(err, test.mockHandshakeErr) {
					t.Errorf("[%d]:[%s] Expected %s handshake error, got %s\n", i, test.name, test.mockHandshakeErr, err)
				}
			} else if test.expectClientErr && err == nil {
				t.Errorf("[%d]:[%s] Expected a client error and got none\n", i, test.name)
			} else if !test.expectClientErr && err != nil {
				t.Errorf("[%d]:[%s] Unexpected error, got %s\n", i, test.name, err)
			}

			mockBroker.Close()
		})
	}
}

func TestSASLPlainAuth(t *testing.T) {
	testTable := []struct {
		name             string
		authidentity     string
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
			name:             "SASL Plain OK server response with authidentity",
			authidentity:     "authid",
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
		test := test
		t.Run(test.name, func(t *testing.T) {
			// mockBroker mocks underlying network logic and broker responses
			mockBroker := NewMockBroker(t, 0)

			mockSASLAuthResponse := NewMockSaslAuthenticateResponse(t).
				SetAuthBytes([]byte(`response_payload`))

			if !errors.Is(test.mockAuthErr, ErrNoError) {
				mockSASLAuthResponse = mockSASLAuthResponse.SetError(test.mockAuthErr)
			}

			mockSASLHandshakeResponse := NewMockSaslHandshakeResponse(t).
				SetEnabledMechanisms([]string{SASLTypePlaintext})

			if !errors.Is(test.mockHandshakeErr, ErrNoError) {
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
			broker.requestsInFlight = metrics.NilCounter{}

			conf := NewTestConfig()
			conf.Net.SASL.Mechanism = SASLTypePlaintext
			conf.Net.SASL.AuthIdentity = test.authidentity
			conf.Net.SASL.Enable = true
			conf.Net.SASL.User = "token"
			conf.Net.SASL.Password = "password"
			conf.Net.SASL.Version = SASLHandshakeV1
			conf.Version = V1_0_0_0

			err := broker.Open(conf)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = broker.Close() })

			_, err = broker.Connected()

			if err == nil {
				for _, rr := range mockBroker.History() {
					switch r := rr.Request.(type) {
					case *SaslAuthenticateRequest:
						x := bytes.SplitN(r.SaslAuthBytes, []byte("\x00"), 3)
						if string(x[0]) != conf.Net.SASL.AuthIdentity {
							t.Errorf("[%d]:[%s] expected %s auth identity, got %s\n", i, test.name, conf.Net.SASL.AuthIdentity, x[0])
						}
						if string(x[1]) != conf.Net.SASL.User {
							t.Errorf("[%d]:[%s] expected %s user, got %s\n", i, test.name, conf.Net.SASL.User, x[1])
						}
						if string(x[2]) != conf.Net.SASL.Password {
							t.Errorf("[%d]:[%s] expected %s password, got %s\n", i, test.name, conf.Net.SASL.Password, x[2])
						}
					}
				}
			}

			if !errors.Is(test.mockAuthErr, ErrNoError) {
				if !errors.Is(err, test.mockAuthErr) {
					t.Errorf("[%d]:[%s] Expected %s auth error, got %s\n", i, test.name, test.mockAuthErr, err)
				}
			} else if !errors.Is(test.mockHandshakeErr, ErrNoError) {
				if !errors.Is(err, test.mockHandshakeErr) {
					t.Errorf("[%d]:[%s] Expected %s handshake error, got %s\n", i, test.name, test.mockHandshakeErr, err)
				}
			} else if test.expectClientErr && err == nil {
				t.Errorf("[%d]:[%s] Expected a client error and got none\n", i, test.name)
			} else if !test.expectClientErr && err != nil {
				t.Errorf("[%d]:[%s] Unexpected error, got %s\n", i, test.name, err)
			}

			mockBroker.Close()
		})
	}
}

// TestSASLReadTimeout ensures that the broker connection won't block forever
// if the remote end never responds after the handshake
func TestSASLReadTimeout(t *testing.T) {
	mockBroker := NewMockBroker(t, 0)
	defer mockBroker.Close()

	mockSASLAuthResponse := NewMockSaslAuthenticateResponse(t).
		SetAuthBytes([]byte(`response_payload`))

	mockBroker.SetHandlerByMap(map[string]MockResponse{
		"SaslAuthenticateRequest": mockSASLAuthResponse,
	})

	broker := NewBroker(mockBroker.Addr())
	{
		broker.requestRate = metrics.NilMeter{}
		broker.outgoingByteRate = metrics.NilMeter{}
		broker.incomingByteRate = metrics.NilMeter{}
		broker.requestSize = metrics.NilHistogram{}
		broker.responseSize = metrics.NilHistogram{}
		broker.responseRate = metrics.NilMeter{}
		broker.requestLatency = metrics.NilHistogram{}
		broker.requestsInFlight = metrics.NilCounter{}
	}

	conf := NewTestConfig()
	{
		conf.Net.ReadTimeout = time.Millisecond
		conf.Net.SASL.Mechanism = SASLTypePlaintext
		conf.Net.SASL.User = "token"
		conf.Net.SASL.Password = "password"
		conf.Net.SASL.Version = SASLHandshakeV1
		conf.Net.SASL.Enable = true
		conf.Version = V1_0_0_0
	}

	err := broker.Open(conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	_, err = broker.Connected()

	var nerr net.Error
	if !(errors.As(err, &nerr) && nerr.Timeout()) {
		t.Errorf("should never happen - expected read timeout got: %v", err)
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
		test := test
		t.Run(test.name, func(t *testing.T) {
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
			broker.requestsInFlight = metrics.NilCounter{}

			conf := NewTestConfig()
			conf.Net.SASL.Mechanism = SASLTypeGSSAPI
			conf.Net.SASL.Enable = true
			conf.Net.SASL.GSSAPI.ServiceName = "kafka"
			conf.Net.SASL.GSSAPI.KerberosConfigPath = "krb5.conf"
			conf.Net.SASL.GSSAPI.Realm = "EXAMPLE.COM"
			conf.Net.SASL.GSSAPI.Username = "kafka"
			conf.Net.SASL.GSSAPI.Password = "kafka"
			conf.Net.SASL.GSSAPI.KeyTabPath = "kafka.keytab"
			conf.Net.SASL.GSSAPI.AuthType = KRB5_USER_AUTH
			conf.Version = V1_0_0_0

			gssapiHandler := KafkaGSSAPIHandler{
				client:         &MockKerberosClient{},
				badResponse:    test.badResponse,
				badKeyChecksum: test.badKeyChecksum,
			}
			mockBroker.SetGSSAPIHandler(gssapiHandler.MockKafkaGSSAPI)
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

			err := broker.Open(conf)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = broker.Close() })

			_, err = broker.Connected()

			if err != nil && test.error != nil {
				if test.error.Error() != err.Error() {
					t.Errorf("[%d] Expected error:%s, got:%s.", i, test.error, err)
				}
			} else if (err == nil && test.error != nil) || (err != nil && test.error == nil) {
				t.Errorf("[%d] Expected error:%s, got:%s.", i, test.error, err)
			}

			mockBroker.Close()
		})
	}
}

func TestBuildClientFirstMessage(t *testing.T) {
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
		test := test
		t.Run(test.name, func(t *testing.T) {
			actual, err := buildClientFirstMessage(test.token)

			if !reflect.DeepEqual(test.expected, actual) {
				t.Errorf("Expected %s, got %s\n", test.expected, actual)
			}
			if test.expectError && err == nil {
				t.Errorf("[%d]:[%s] Expected an error but did not get one", i, test.name)
			}
			if !test.expectError && err != nil {
				t.Errorf("[%d]:[%s] Expected no error but got %s\n", i, test.name, err)
			}
		})
	}
}

func TestKip368ReAuthenticationSuccess(t *testing.T) {
	sessionLifetimeMs := int64(100)

	mockBroker := NewMockBroker(t, 0)

	countSaslAuthRequests := func() (count int) {
		for _, rr := range mockBroker.History() {
			switch rr.Request.(type) {
			case *SaslAuthenticateRequest:
				count++
			}
		}
		return
	}

	mockSASLAuthResponse := NewMockSaslAuthenticateResponse(t).
		SetAuthBytes([]byte(`response_payload`)).
		SetSessionLifetimeMs(sessionLifetimeMs)

	mockSASLHandshakeResponse := NewMockSaslHandshakeResponse(t).
		SetEnabledMechanisms([]string{SASLTypePlaintext})

	mockApiVersions := NewMockApiVersionsResponse(t)

	mockBroker.SetHandlerByMap(map[string]MockResponse{
		"SaslAuthenticateRequest": mockSASLAuthResponse,
		"SaslHandshakeRequest":    mockSASLHandshakeResponse,
		"ApiVersionsRequest":      mockApiVersions,
	})

	broker := NewBroker(mockBroker.Addr())

	conf := NewTestConfig()
	conf.Net.SASL.Enable = true
	conf.Net.SASL.Mechanism = SASLTypePlaintext
	conf.Net.SASL.Version = SASLHandshakeV1
	conf.Net.SASL.AuthIdentity = "authid"
	conf.Net.SASL.User = "token"
	conf.Net.SASL.Password = "password"

	broker.conf = conf
	broker.conf.Version = V2_2_0_0

	err := broker.Open(conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	connected, err := broker.Connected()
	if err != nil || !connected {
		t.Fatal(err)
	}

	actualSaslAuthRequests := countSaslAuthRequests()
	if actualSaslAuthRequests != 1 {
		t.Fatalf("unexpected number of SaslAuthRequests during initial authentication: %d", actualSaslAuthRequests)
	}

	timeout := time.After(time.Duration(sessionLifetimeMs) * time.Millisecond)

loop:
	for actualSaslAuthRequests < 2 {
		select {
		case <-timeout:
			break loop
		default:
			time.Sleep(10 * time.Millisecond)
			// put some traffic on the wire
			_, err = broker.ApiVersions(&ApiVersionsRequest{})
			if err != nil {
				t.Fatal(err)
			}
			actualSaslAuthRequests = countSaslAuthRequests()
		}
	}

	if actualSaslAuthRequests < 2 {
		t.Fatalf("sasl reauth has not occurred within expected timeframe")
	}

	mockBroker.Close()
}

func TestKip368ReAuthenticationFailure(t *testing.T) {
	sessionLifetimeMs := int64(100)

	mockBroker := NewMockBroker(t, 0)

	mockSASLAuthResponse := NewMockSaslAuthenticateResponse(t).
		SetAuthBytes([]byte(`response_payload`)).
		SetSessionLifetimeMs(sessionLifetimeMs)

	mockSASLAuthErrorResponse := NewMockSaslAuthenticateResponse(t).
		SetError(ErrSASLAuthenticationFailed)

	mockSASLHandshakeResponse := NewMockSaslHandshakeResponse(t).
		SetEnabledMechanisms([]string{SASLTypePlaintext})

	mockApiVersions := NewMockApiVersionsResponse(t)

	mockBroker.SetHandlerByMap(map[string]MockResponse{
		"SaslAuthenticateRequest": mockSASLAuthResponse,
		"SaslHandshakeRequest":    mockSASLHandshakeResponse,
		"ApiVersionsRequest":      mockApiVersions,
	})

	broker := NewBroker(mockBroker.Addr())

	conf := NewTestConfig()
	conf.Net.SASL.Enable = true
	conf.Net.SASL.Mechanism = SASLTypePlaintext
	conf.Net.SASL.Version = SASLHandshakeV1
	conf.Net.SASL.AuthIdentity = "authid"
	conf.Net.SASL.User = "token"
	conf.Net.SASL.Password = "password"

	broker.conf = conf
	broker.conf.Version = V2_2_0_0

	err := broker.Open(conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	connected, err := broker.Connected()
	if err != nil || !connected {
		t.Fatal(err)
	}

	mockBroker.SetHandlerByMap(map[string]MockResponse{
		"SaslAuthenticateRequest": mockSASLAuthErrorResponse,
		"SaslHandshakeRequest":    mockSASLHandshakeResponse,
		"ApiVersionsRequest":      mockApiVersions,
	})

	timeout := time.After(time.Duration(sessionLifetimeMs) * time.Millisecond)

	var apiVersionError error
loop:
	for apiVersionError == nil {
		select {
		case <-timeout:
			break loop
		default:
			time.Sleep(10 * time.Millisecond)
			// put some traffic on the wire
			_, apiVersionError = broker.ApiVersions(&ApiVersionsRequest{})
		}
	}

	if !errors.Is(apiVersionError, ErrSASLAuthenticationFailed) {
		t.Fatalf("sasl reauth has not failed in the expected way %v", apiVersionError)
	}

	mockBroker.Close()
}

// We're not testing encoding/decoding here, so most of the requests/responses will be empty for simplicity's sake
var brokerTestTable = []struct {
	version  KafkaVersion
	name     string
	response []byte
	runner   func(*testing.T, *Broker)
}{
	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
		"ProduceRequest (NoResponse) using AsyncProduce",
		[]byte{},
		func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = NoResponse
			err := broker.AsyncProduce(&request, nil)
			if err != nil {
				t.Error(err)
			}
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
		"ProduceRequest (WaitForLocal) using AsyncProduce",
		[]byte{0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = WaitForLocal
			produceResPromise := newProduceResponsePromise()
			err := broker.AsyncProduce(&request, produceResPromise.callback)
			if err != nil {
				t.Error(err)
			}
			response, err := produceResPromise.Get()
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("Produce request without NoResponse got no response!")
			}
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V0_10_0_0,
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
		},
	},

	{
		V1_1_0_0,
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
		},
	},

	{
		V2_4_0_0,
		"DeleteOffsetsRequest",
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		func(t *testing.T, broker *Broker) {
			request := DeleteOffsetsRequest{}
			response, err := broker.DeleteOffsets(&request)
			if err != nil {
				t.Error(err)
			}
			if response == nil {
				t.Error("DeleteGroups request got no response!")
			}
		},
	},
}

// We are testing the handling of failed request or corrupt responses.
var brokerFailedReqTestTable = []struct {
	version    KafkaVersion
	name       string
	stopBroker bool
	response   []byte
	runner     func(*testing.T, *Broker)
}{
	{
		version:    V0_10_0_0,
		name:       "ProduceRequest (NoResponse) using AsyncProduce and stopped broker",
		stopBroker: true,
		runner: func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = NoResponse
			err := broker.AsyncProduce(&request, nil)
			if err == nil {
				t.Fatal("Expected a non nil error because broker is not listening")
			}
			t.Log("Got error:", err)
		},
	},

	{
		version:    V0_10_0_0,
		name:       "ProduceRequest (WaitForLocal) using AsyncProduce and stopped broker",
		stopBroker: true,
		runner: func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = WaitForLocal
			err := broker.AsyncProduce(&request, nil)
			if err == nil {
				t.Fatal("Expected a non nil error because broker is not listening")
			}
			t.Log("Got error:", err)
		},
	},

	{
		version: V0_10_0_0,
		name:    "ProduceRequest (WaitForLocal) using AsyncProduce and no response",
		// A nil response means the mock broker will ignore the request leading to a read timeout
		response: nil,
		runner: func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = WaitForLocal
			produceResPromise := newProduceResponsePromise()
			err := broker.AsyncProduce(&request, produceResPromise.callback)
			if err != nil {
				t.Error(err)
			}
			response, err := produceResPromise.Get()
			if err == nil {
				t.Fatal("Expected a non nil error because broker is not listening")
			}
			t.Log("Got error:", err)
			if response != nil {
				t.Error("Produce request should have failed, got response:", response)
			}
		},
	},

	{
		version: V0_10_0_0,
		name:    "ProduceRequest (WaitForLocal) using AsyncProduce and corrupt response",
		// Corrupt response (3 bytes vs 4)
		response: []byte{0x00, 0x00, 0x00},
		runner: func(t *testing.T, broker *Broker) {
			request := ProduceRequest{}
			request.RequiredAcks = WaitForLocal
			produceResPromise := newProduceResponsePromise()
			err := broker.AsyncProduce(&request, produceResPromise.callback)
			if err != nil {
				t.Error(err)
			}
			response, err := produceResPromise.Get()
			if err == nil {
				t.Fatal(err)
			}
			t.Log("Got error:", err)
			if response != nil {
				t.Error("Produce request should have failed, got response:", response)
			}
		},
	},
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

	// Check that there is no more requests in flight
	metricValidators.registerForAllBrokers(broker, counterValidator("requests-in-flight", 0))

	// Run the validators
	metricValidators.run(t, broker.conf.MetricRegistry)
}

func BenchmarkBroker_Open(b *testing.B) {
	mb := NewMockBroker(nil, 0)
	defer mb.Close()
	broker := NewBroker(mb.Addr())
	// Set the broker id in order to validate local broker metrics
	broker.id = 0
	metrics.UseNilMetrics = false
	conf := NewTestConfig()
	conf.Version = V1_0_0_0
	for i := 0; i < b.N; i++ {
		err := broker.Open(conf)
		if err != nil {
			b.Fatal(err)
		}
		broker.Close()
	}
}

func BenchmarkBroker_No_Metrics_Open(b *testing.B) {
	mb := NewMockBroker(nil, 0)
	defer mb.Close()
	broker := NewBroker(mb.Addr())
	broker.id = 0
	metrics.UseNilMetrics = true
	conf := NewTestConfig()
	conf.Version = V1_0_0_0
	for i := 0; i < b.N; i++ {
		err := broker.Open(conf)
		if err != nil {
			b.Fatal(err)
		}
		broker.Close()
	}
}

func Test_handleThrottledResponse(t *testing.T) {
	mb := NewMockBroker(nil, 0)
	defer mb.Close()
	broker := NewBroker(mb.Addr())
	broker.id = 0
	conf := NewTestConfig()
	conf.Version = V1_0_0_0
	throttleTimeMs := 100
	throttleTime := time.Duration(throttleTimeMs) * time.Millisecond
	tests := []struct {
		name        string
		response    protocolBody
		expectDelay bool
	}{
		{
			name: "throttled response w/millisecond field",
			response: &MetadataResponse{
				ThrottleTimeMs: int32(throttleTimeMs),
			},
			expectDelay: true,
		},
		{
			name: "not throttled response w/millisecond field",
			response: &MetadataResponse{
				ThrottleTimeMs: 0,
			},
		},
		{
			name: "throttled response w/time.Duration field",
			response: &ProduceResponse{
				ThrottleTime: throttleTime,
			},
			expectDelay: true,
		},
		{
			name: "not throttled response w/time.Duration field",
			response: &ProduceResponse{
				ThrottleTime: time.Duration(0),
			},
		},
		{
			name:     "not throttled response with no throttle time field",
			response: &SaslHandshakeResponse{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker.metricRegistry = metrics.NewRegistry()
			broker.brokerThrottleTime = broker.registerHistogram("throttle-time-in-ms")
			startTime := time.Now()
			broker.handleThrottledResponse(tt.response)
			broker.waitIfThrottled()
			if tt.expectDelay {
				if time.Since(startTime) < throttleTime {
					t.Fatal("expected throttling to cause delay")
				}
				if broker.brokerThrottleTime.Min() != int64(throttleTimeMs) {
					t.Fatal("expected throttling to update metrics")
				}
			} else {
				if time.Since(startTime) > throttleTime {
					t.Fatal("expected no throttling delay")
				}
				if broker.brokerThrottleTime.Count() != 0 {
					t.Fatal("expected no metrics update")
				}
			}
		})
	}
	t.Run("test second throttle timer overrides first", func(t *testing.T) {
		broker.metricRegistry = metrics.NewRegistry()
		broker.brokerThrottleTime = broker.registerHistogram("throttle-time-in-ms")
		broker.handleThrottledResponse(&MetadataResponse{
			ThrottleTimeMs: int32(throttleTimeMs),
		})
		firstTimer := broker.throttleTimer
		broker.handleThrottledResponse(&MetadataResponse{
			ThrottleTimeMs: int32(throttleTimeMs * 2),
		})
		if firstTimer.Stop() {
			t.Fatal("expected first timer to be stopped")
		}
		startTime := time.Now()
		broker.waitIfThrottled()
		if time.Since(startTime) < throttleTime*2 {
			t.Fatal("expected throttling to use second delay")
		}
		if broker.brokerThrottleTime.Min() != int64(throttleTimeMs) {
			t.Fatal("expected throttling to update metrics")
		}
		if broker.brokerThrottleTime.Max() != int64(throttleTimeMs*2) {
			t.Fatal("expected throttling to update metrics")
		}
	})
}
