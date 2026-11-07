// Package tlstest generates certificates for TLS and mTLS tests.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"
)

// Pair is a PEM certificate and its PEM private key.
type Pair struct {
	Cert string
	Key  string
}

// TLS returns the pair as a certificate for a tls.Config.
func (p Pair) TLS(t testing.TB) tls.Certificate {
	t.Helper()
	cert, err := tls.X509KeyPair([]byte(p.Cert), []byte(p.Key))
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// Certs holds a CA, a server certificate for localhost signed by it, a client
// certificate signed by it, and a client certificate from another CA.
type Certs struct {
	CA        string
	Server    Pair
	Client    Pair
	Untrusted Pair
}

type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  string
}

// New generates a fresh set of certificates.
func New(t testing.TB) *Certs {
	t.Helper()
	ca := newCA(t, "tlstest CA")
	other := newCA(t, "tlstest other CA")
	return &Certs{
		CA: ca.pem,
		Server: ca.issue(t, &x509.Certificate{
			Subject:     pkix.Name{CommonName: "localhost"},
			DNSNames:    []string{"localhost"},
			IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}),
		Client:    ca.issue(t, clientTemplate()),
		Untrusted: other.issue(t, clientTemplate()),
	}
}

// ClientTLS returns a client config that trusts the CA and presents client,
// if given.
func (c *Certs) ClientTLS(t testing.TB, client *Pair) *tls.Config {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(c.CA))
	cfg := &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	if client != nil {
		cfg.Certificates = []tls.Certificate{client.TLS(t)}
	}
	return cfg
}

func clientTemplate() *x509.Certificate {
	return &x509.Certificate{
		Subject:     pkix.Name{CommonName: "client"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
}

func newCA(t testing.TB, name string) *authority {
	t.Helper()
	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          serial(t),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &authority{cert: cert, key: key, pem: encode("CERTIFICATE", der)}
}

func (a *authority) issue(t testing.TB, tmpl *x509.Certificate) Pair {
	t.Helper()
	key := newKey(t)
	tmpl.SerialNumber = serial(t)
	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().Add(24 * time.Hour)
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return Pair{Cert: encode("CERTIFICATE", der), Key: encode("PRIVATE KEY", keyDER)}
}

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func serial(t testing.TB) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func encode(blockType string, der []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}))
}
