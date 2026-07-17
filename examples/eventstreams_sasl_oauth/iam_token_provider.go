package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// grantTypeAPIKey is the IBM IAM grant type used to exchange an API key for an
// access token, matching the Java client's
// grant_type="urn:ibm:params:oauth:grant-type:apikey".
const grantTypeAPIKey = "urn:ibm:params:oauth:grant-type:apikey"

// expiryWindow is how long before the reported expiry we proactively refresh,
// so we never hand the broker a token that is about to expire.
const expiryWindow = 60 * time.Second

// iamTokenProvider implements sarama.AccessTokenProvider. It performs the IBM
// Cloud IAM apikey -> bearer token exchange and caches the result until it is
// close to expiry. This is the Go equivalent of the Event Streams Java
// IAMOAuthBearerLoginCallbackHandler / IAMAPIKeyTokenRetriever.
type iamTokenProvider struct {
	apiKey        string
	tokenEndpoint string
	httpClient    *http.Client

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

func newIAMTokenProvider(apiKey, tokenEndpoint string) sarama.AccessTokenProvider {
	return &iamTokenProvider{
		apiKey:        apiKey,
		tokenEndpoint: tokenEndpoint,
		httpClient:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Token returns a cached token if it is still valid, otherwise it fetches a
// fresh one from the IAM token endpoint.
func (p *iamTokenProvider) Token() (*sarama.AccessToken, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Sarama calls Token() on the initial SASL handshake and again on every
	// re-authentication (KIP-368, triggered by the broker's
	// connections.max.reauth.ms). While the cached token is still valid we
	// reuse it, so re-authentication does not force a fresh IAM exchange.
	if p.token != "" && time.Now().Before(p.expiresAt) {
		log.Printf("[token] reusing cached IAM token (valid for another %s)",
			time.Until(p.expiresAt).Round(time.Second))
		return &sarama.AccessToken{Token: p.token}, nil
	}

	log.Println("[token] requesting a new IAM access token (initial auth or re-authentication)")
	token, expiresIn, err := p.fetchToken()
	if err != nil {
		return nil, err
	}

	p.token = token
	p.expiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second).Add(-expiryWindow)
	log.Printf("[token] obtained new IAM token (expires_in=%ds, will refresh after %s)",
		expiresIn, p.expiresAt.Format(time.RFC3339))

	return &sarama.AccessToken{Token: p.token}, nil
}

func (p *iamTokenProvider) fetchToken() (token string, expiresIn int64, err error) {
	form := url.Values{}
	form.Set("grant_type", grantTypeAPIKey)
	form.Set("apikey", p.apiKey)

	req, err := http.NewRequest(http.MethodPost, p.tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("building IAM token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("calling IAM token endpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, fmt.Errorf("reading IAM token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("IAM token endpoint returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		ErrorCode   string `json:"errorCode"`
		ErrorMsg    string `json:"errorMessage"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", 0, fmt.Errorf("parsing IAM token response: %w", err)
	}
	if parsed.AccessToken == "" {
		return "", 0, fmt.Errorf("IAM token response did not contain an access_token (errorCode=%q errorMessage=%q)",
			parsed.ErrorCode, parsed.ErrorMsg)
	}

	return parsed.AccessToken, parsed.ExpiresIn, nil
}
