package tls

import "crypto/tls"

func NewConfig(clientCert, clientKey string) (*tls.Config, error) {
	tlsConfig := tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if clientCert != "" && clientKey != "" {
		cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			return &tlsConfig, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return &tlsConfig, nil
}
