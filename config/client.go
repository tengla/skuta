package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

type ClientConfig struct {
	GrpcHost   string           `json:"grpcHost"`
	GrpcPort   int              `json:"grpcPort"`
	ApiAddress string           `json:"apiAddress"`
	AuthToken  string           `json:"authToken"`
	ClientID   string           `json:"clientId"`
	TLS        *ClientTLSConfig `json:"tls"`
}

// ClientTLSConfig is how CLI commands connect to a server that has the
// security section set. Ca is the CA that signed the server's certificate,
// or empty to use the system's trusted CAs. Cert and Key are the client
// certificate a server in mTLS mode requires.
type ClientTLSConfig struct {
	Enable bool            `json:"enable"`
	Ca     *ResourceConfig `json:"ca"`
	Cert   *ResourceConfig `json:"cert"`
	Key    *ResourceConfig `json:"key"`
	// ServerName overrides the name checked against the server's
	// certificate, which is grpcHost by default.
	ServerName string `json:"serverName"`
}

func defaultClientConfig() *ClientConfig {
	bindViperEnv(
		"Client.Tls.Enable",
		"Client.Tls.Ca.Filename",
		"Client.Tls.Ca.Data",
		"Client.Tls.Cert.Filename",
		"Client.Tls.Cert.Data",
		"Client.Tls.Key.Filename",
		"Client.Tls.Key.Data",
		"Client.Tls.ServerName",
	)
	return &ClientConfig{
		GrpcHost:   "localhost",
		GrpcPort:   50000,
		ApiAddress: "http://localhost:8080",
		AuthToken:  "",
		ClientID:   "",
		TLS: &ClientTLSConfig{
			Enable:     false,
			Ca:         defaultResourceConfig(),
			Cert:       defaultResourceConfig(),
			Key:        defaultResourceConfig(),
			ServerName: "",
		},
	}
}

func (c *ClientConfig) Validate() error {
	if c.GrpcHost == "" {
		return NewConfigurationErrorf("bad client grpc host configuration: value cannot be empty")
	}
	if err := validatePort(c.GrpcPort); err != nil {
		return NewConfigurationErrorf("bad client grpc port configuration: %s", err.Error())
	}
	if err := validateURL(c.ApiAddress); err != nil {
		return NewConfigurationErrorf("bad client api address configuration: %s", err.Error())
	}
	if c.TLS != nil && c.TLS.Enable {
		if _, err := c.TLS.Config(); err != nil {
			return NewConfigurationErrorf("bad client tls configuration: %s", err.Error())
		}
	}
	return nil
}

// Config builds the client's TLS settings, or returns nil when TLS is off.
func (t *ClientTLSConfig) Config() (*tls.Config, error) {
	if t == nil || !t.Enable {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.ServerName}
	if !empty(t.Ca) {
		caBlock, err := t.Ca.Get()
		if err != nil {
			return nil, fmt.Errorf("invalid ca data: %s", err.Error())
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBlock) {
			return nil, fmt.Errorf("invalid ca data: no PEM certificates found")
		}
		cfg.RootCAs = pool
	}
	if empty(t.Cert) != empty(t.Key) {
		return nil, fmt.Errorf("cert and key must be set together")
	}
	if !empty(t.Cert) {
		certBlock, err := t.Cert.Get()
		if err != nil {
			return nil, fmt.Errorf("invalid cert data: %s", err.Error())
		}
		keyBlock, err := t.Key.Get()
		if err != nil {
			return nil, fmt.Errorf("invalid key data: %s", err.Error())
		}
		cert, err := tls.X509KeyPair(certBlock, keyBlock)
		if err != nil {
			return nil, fmt.Errorf("invalid cert and key pair: %s", err.Error())
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func empty(r *ResourceConfig) bool {
	return r == nil || r.Empty()
}
