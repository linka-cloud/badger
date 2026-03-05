package replication

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestWithTickMinGuard(t *testing.T) {
	o := defaultOptions()
	WithTick(100)(&o)
	if o.Tick != 0 {
		t.Fatalf("tick should remain unset for <=100ms")
	}
	WithTick(250)(&o)
	if o.Tick != 250*time.Millisecond {
		t.Fatalf("unexpected tick: %s", o.Tick)
	}
}

func TestNormalizeOptionsDerivations(t *testing.T) {
	o := Options{Name: "node-9", Addrs: []string{"127.0.0.1:7946"}, Tick: 300 * time.Millisecond}
	normalizeOptions(&o)
	if o.NodeID != 9 {
		t.Fatalf("unexpected node id: %d", o.NodeID)
	}
	if o.GRPCPort != 19009 || o.GossipPort != 7954 {
		t.Fatalf("unexpected ports grpc=%d gossip=%d", o.GRPCPort, o.GossipPort)
	}
	if o.GRPCAddr == "" || o.MemberlistBindAddr == "" {
		t.Fatalf("addresses should be derived")
	}
	if len(o.Join) != 1 || o.Join[0] != "127.0.0.1:7946" {
		t.Fatalf("join should be derived from addrs: %#v", o.Join)
	}
	if o.StateDir == "" {
		t.Fatalf("state dir should be derived")
	}
	if o.HeartbeatInterval != o.Tick {
		t.Fatalf("heartbeat should follow tick: %s vs %s", o.HeartbeatInterval, o.Tick)
	}
	if o.ElectionTimeout != o.Tick {
		t.Fatalf("election timeout should follow tick")
	}
	if o.LeaderTimeout != 20*o.Tick {
		t.Fatalf("leader timeout should be 20*tick")
	}
}

func TestTLSMissingServerMaterial(t *testing.T) {
	o := Options{}
	WithServerCert([]byte("cert"))(&o)
	_, err := o.TLS()
	if err == nil || !strings.Contains(err.Error(), "missing server key") {
		t.Fatalf("expected missing server key error, got: %v", err)
	}

	o = Options{}
	WithServerKey([]byte("key"))(&o)
	_, err = o.TLS()
	if err == nil || !strings.Contains(err.Error(), "missing server certificate") {
		t.Fatalf("expected missing server certificate error, got: %v", err)
	}
}

func TestTLSWithExplicitConfig(t *testing.T) {
	c := &tls.Config{MinVersion: tls.VersionTLS13}
	o := Options{}
	WithTLSConfig(c)(&o)
	out, err := o.TLS()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != c {
		t.Fatalf("expected same tls config pointer")
	}
}

func TestTLSBuildsServerAndClientAuth(t *testing.T) {
	certPEM, keyPEM := testCertPair(t)
	o := Options{}
	WithServerCert(certPEM)(&o)
	WithServerKey(keyPEM)(&o)
	WithClientCert(certPEM)(&o)
	WithClientKey(keyPEM)(&o)
	WithClientCA(certPEM)(&o)

	tlsCfg, err := o.TLS()
	if err != nil {
		t.Fatalf("unexpected tls error: %v", err)
	}
	if len(tlsCfg.Certificates) != 2 {
		t.Fatalf("expected two certificates, got %d", len(tlsCfg.Certificates))
	}
	if tlsCfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("unexpected client auth mode: %v", tlsCfg.ClientAuth)
	}
}

func testCertPair(t *testing.T) ([]byte, []byte) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	return certPEM, keyPEM
}

func TestWithExtraServicesAppends(t *testing.T) {
	o := defaultOptions()
	s1 := func(grpc.ServiceRegistrar) {}
	s2 := func(grpc.ServiceRegistrar) {}
	WithExtraServices(s1)(&o)
	WithExtraServices(s2)(&o)
	if len(o.ExtraServices) != 2 {
		t.Fatalf("expected 2 extra services, got %d", len(o.ExtraServices))
	}
}
