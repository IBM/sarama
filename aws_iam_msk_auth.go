package sarama

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	sign "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
)

type AWSMSKIAMConfig struct {
	Region string
	Expiry time.Duration
}

func buildAwsIAMPayload(addr, useragent string, cfg AWSMSKIAMConfig) ([]byte, error) {
	// Use default config loader
	// Full doc: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/#specifying-credentials
	awsCfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}

	credentials, err := awsCfg.Credentials.Retrieve(context.Background())
	if err != nil {
		return nil, err
	}

	signer := sign.NewSigner()

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	var action = "kafka-cluster:Connect"

	if cfg.Expiry == time.Duration(0) {
		cfg.Expiry = 5 * time.Minute
	}

	query := url.Values{
		"Action":        {action},
		"X-Amz-Expires": {strconv.FormatInt(int64(cfg.Expiry/time.Second), 10)},
	}

	url := url.URL{
		Host:     host,
		Path:     "/",
		RawQuery: query.Encode(),
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	signedUri, _, err := signer.PresignHTTP(context.TODO(), credentials, req, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "kafka-cluster", cfg.Region, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	payload := map[string]string{
		"version":    IAMAuthVersion,
		"host":       host,
		"user-agent": useragent,
		"action":     action,
	}

	parsedUri, err := url.Parse(signedUri)
	if err != nil {
		return nil, err
	}

	for key, vals := range parsedUri.Query() {
		payload[strings.ToLower(key)] = vals[0]
	}

	return json.Marshal(payload)
}
