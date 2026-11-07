package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

type SecurityModeType int

const (
	SecurityModeNone SecurityModeType = iota
	SecurityModeTLS
	SecurityModeMTLS
)

type SecurityConfig struct {
	Cert *ResourceConfig `json:"cert"`
	Key  *ResourceConfig `json:"key"`
	Ca   *ResourceConfig `json:"ca"`
}

func defaultSecurityConfig() *SecurityConfig {
	bindViperEnv(
		"Security.Cert.Filename",
		"Security.Cert.Data",
		"Security.Key.Filename",
		"Security.Key.Data",
		"Security.Ca.Filename",
		"Security.Ca.Data",
	)
	return &SecurityConfig{
		Cert: defaultResourceConfig(),
		Key:  defaultResourceConfig(),
		Ca:   defaultResourceConfig(),
	}
}

func (s *SecurityConfig) Validate() error {
	switch s.Mode() {
	case SecurityModeNone:
		return nil
	case SecurityModeTLS:
		_, err := s.Cert.Get()
		if err != nil {
			return fmt.Errorf("bad security TLS configuration: Cert data is invalid: %s", err.Error())
		}
		_, err = s.Key.Get()
		if err != nil {
			return fmt.Errorf("bad security TLS configuration: Key data is invalid: %s", err.Error())
		}
		return nil
	case SecurityModeMTLS:
		_, err := s.Cert.Get()
		if err != nil {
			return fmt.Errorf("bad security mTLS configuration: Cert data is invalid: %s", err.Error())
		}
		_, err = s.Key.Get()
		if err != nil {
			return fmt.Errorf("bad security mTLS configuration: Key data is invalid: %s", err.Error())
		}
		_, err = s.Ca.Get()
		if err != nil {
			return fmt.Errorf("bad security mTLS configuration: CA data is invalid: %s", err.Error())
		}
		return nil
	}

	return fmt.Errorf("bad security configuration: invalid mode")
}

func (s *SecurityConfig) Mode() SecurityModeType {

	if s.Key != nil && !s.Key.Empty() && s.Cert != nil && !s.Cert.Empty() {
		if s.Ca != nil && !s.Ca.Empty() {
			return SecurityModeMTLS
		}
		return SecurityModeTLS
	}
	return SecurityModeNone
}

// TLSConfig builds the server's TLS settings, or returns nil when TLS is off.
// In mTLS mode clients must present a certificate signed by Ca.
func (s *SecurityConfig) TLSConfig() (*tls.Config, error) {
	mode := s.Mode()
	if mode == SecurityModeNone {
		return nil, nil
	}
	certBlock, err := s.Cert.Get()
	if err != nil {
		return nil, fmt.Errorf("invalid cert data: %s", err.Error())
	}
	keyBlock, err := s.Key.Get()
	if err != nil {
		return nil, fmt.Errorf("invalid key data: %s", err.Error())
	}
	cert, err := tls.X509KeyPair(certBlock, keyBlock)
	if err != nil {
		return nil, fmt.Errorf("invalid cert and key pair: %s", err.Error())
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if mode == SecurityModeMTLS {
		caBlock, err := s.Ca.Get()
		if err != nil {
			return nil, fmt.Errorf("invalid ca data: %s", err.Error())
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBlock) {
			return nil, fmt.Errorf("invalid ca data: no PEM certificates found")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
