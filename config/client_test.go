package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tengla/skuta/pkg/tlstest"
)

func Test_ClientTLSLoadEnvironmentVars(t *testing.T) {
	for key, value := range map[string]string{
		"Client.Tls.Enable":        "true",
		"Client.Tls.Ca.Filename":   "ca.pem",
		"Client.Tls.Cert.Filename": "client.pem",
		"Client.Tls.Key.Filename":  "client-key.pem",
		"Client.Tls.ServerName":    "broker.example",
	} {
		t.Setenv(convertEnvFormat(key), value)
	}
	appConfig := getConfigRecord()
	require.EqualValues(t, &ClientTLSConfig{
		Enable:     true,
		Ca:         &ResourceConfig{Filename: "ca.pem"},
		Cert:       &ResourceConfig{Filename: "client.pem"},
		Key:        &ResourceConfig{Filename: "client-key.pem"},
		ServerName: "broker.example",
	}, appConfig.Client.TLS)
}

func TestClientTLSConfig(t *testing.T) {
	certs := tlstest.New(t)
	data := func(s string) *ResourceConfig { return &ResourceConfig{Data: s} }

	t.Run("disabled", func(t *testing.T) {
		cfg, err := (&ClientTLSConfig{Ca: data(certs.CA)}).Config()
		require.NoError(t, err)
		require.Nil(t, cfg)
	})
	t.Run("system CAs", func(t *testing.T) {
		cfg, err := (&ClientTLSConfig{Enable: true}).Config()
		require.NoError(t, err)
		require.Nil(t, cfg.RootCAs)
		require.Empty(t, cfg.Certificates)
	})
	t.Run("CA and client certificate", func(t *testing.T) {
		cfg, err := (&ClientTLSConfig{Enable: true, Ca: data(certs.CA), Cert: data(certs.Client.Cert), Key: data(certs.Client.Key), ServerName: "localhost"}).Config()
		require.NoError(t, err)
		require.NotNil(t, cfg.RootCAs)
		require.Len(t, cfg.Certificates, 1)
		require.Equal(t, "localhost", cfg.ServerName)
	})
	t.Run("cert without key", func(t *testing.T) {
		_, err := (&ClientTLSConfig{Enable: true, Cert: data(certs.Client.Cert)}).Config()
		require.ErrorContains(t, err, "cert and key must be set together")
	})
	t.Run("invalid CA", func(t *testing.T) {
		_, err := (&ClientTLSConfig{Enable: true, Ca: data("not a certificate")}).Config()
		require.ErrorContains(t, err, "invalid ca data")
	})
	t.Run("Validate reports it", func(t *testing.T) {
		c := defaultClientConfig()
		c.TLS = &ClientTLSConfig{Enable: true, Key: data(certs.Client.Key)}
		require.ErrorContains(t, c.Validate(), "bad client tls configuration")
	})
}

func TestRedactedMasksClientTLSKey(t *testing.T) {
	c := GetDefaultConfig()
	c.Client.TLS.Key.Data = "private key"
	require.Equal(t, redactedValue, c.Redacted().Client.TLS.Key.Data)
	require.Equal(t, "private key", c.Client.TLS.Key.Data)
}
