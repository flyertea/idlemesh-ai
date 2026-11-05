package ha

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const (
	peerURI         = "urn:idlemesh:raft:"
	maxIncoming     = 64
	rosterProtoBase = "idlemesh-raft-v1-"
)

var (
	peerIDPattern   = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)
	dnsLabelPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
)

func normalizePeers(localID string, list []Peer) ([]Peer, Peer, error) {
	if len(list) != 3 {
		return nil, Peer{}, errors.New("Raft requires exactly three voters")
	}
	peers := make([]Peer, len(list))
	var local Peer
	ids := make(map[string]bool, len(list))
	addresses := make(map[string]bool, len(list))
	for index, peer := range list {
		if !peerIDPattern.MatchString(peer.ID) || ids[peer.ID] {
			return nil, Peer{}, fmt.Errorf("invalid or duplicate Raft voter ID %q", peer.ID)
		}
		address, err := normalizeAddress(peer.Address)
		if err != nil || addresses[address] {
			return nil, Peer{}, fmt.Errorf("invalid or duplicate Raft address %q for voter %s", peer.Address, peer.ID)
		}
		peer.Address = address
		peers[index] = peer
		ids[peer.ID], addresses[address] = true, true
		if peer.ID == localID {
			local = peer
		}
	}
	if local.ID == "" {
		return nil, Peer{}, errors.New("Raft voter roster must include the local ID")
	}
	slices.SortFunc(peers, func(a, b Peer) int { return strings.Compare(a.ID, b.ID) })
	return peers, local, nil
}

func normalizeAddress(address string) (string, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", errors.New("advertised port must be between 1 and 65535")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.IsUnspecified() || ip.Zone() != "" {
			return "", errors.New("advertise a reachable, unscoped address")
		}
		host = ip.Unmap().String()
	} else {
		host = strings.ToLower(strings.TrimSuffix(host, "."))
		if len(host) > 253 {
			return "", errors.New("advertised hostname is too long")
		}
		for _, label := range strings.Split(host, ".") {
			if !dnsLabelPattern.MatchString(label) {
				return "", errors.New("invalid advertised hostname")
			}
		}
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

type peerAddr string

func (a peerAddr) Network() string { return "tcp" }
func (a peerAddr) String() string  { return string(a) }

type peerStream struct {
	net.Listener
	advertised peerAddr
	clientTLS  *tls.Config
	serverTLS  *tls.Config
	targets    map[raft.ServerAddress]Peer
	mu         sync.Mutex
	incoming   map[*peerConn]struct{}
}

func newPeerStream(cfg Config, peers []Peer, local Peer) (*peerStream, error) {
	if cfg.TLSCert == "" || cfg.TLSKey == "" || cfg.TLSCA == "" {
		return nil, errors.New("Raft requires a peer TLS certificate, private key, and cluster CA")
	}
	caPEM, err := os.ReadFile(cfg.TLSCA)
	if err != nil {
		return nil, fmt.Errorf("read Raft TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("Raft TLS CA file contains no PEM certificates")
	}
	certificate, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("load Raft TLS identity: %w", err)
	}
	if err := verifyLocalCert(certificate, roots, local); err != nil {
		return nil, fmt.Errorf("invalid Raft TLS identity for %s: %w", local.ID, err)
	}

	var rosterScope any = peers
	if cfg.ReplaceID != "" {
		// An object starts in a separate hash domain from the old roster array.
		// Include the full replacement plan and exact authentication policy.
		rosterScope = struct {
			Peers  []Peer `json:"peers"`
			OldID  string `json:"old_id"`
			NewID  string `json:"new_id"`
			Policy []byte `json:"policy"`
		}{peers, cfg.ReplaceID, cfg.ReplaceNewID, []byte(cfg.Policy)}
	}
	roster, err := json.Marshal(rosterScope)
	if err != nil {
		return nil, err
	}
	if cfg.ReplaceID == "" && cfg.Policy != "" {
		// JSON never contains a literal NUL, so the roster and policy are
		// unambiguous. The empty policy keeps the original v0.3 wire format.
		roster = append(roster, 0)
		roster = append(roster, cfg.Policy...)
	}
	digest := sha256.Sum256(roster)
	protocol := rosterProtoBase + hex.EncodeToString(digest[:])
	baseTLS := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{protocol},
	}
	clientTLS := baseTLS.Clone()
	clientTLS.RootCAs = roots
	serverTLS := baseTLS.Clone()
	serverTLS.ClientCAs = roots
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	serverTLS.VerifyConnection = func(state tls.ConnectionState) error {
		if state.NegotiatedProtocol != protocol {
			return errors.New("Raft peer roster or policy does not match")
		}
		if len(state.PeerCertificates) != 0 {
			for _, peer := range peers {
				if peer.ID != local.ID && verifyPeerCert(state.PeerCertificates[0], peer) == nil {
					return nil
				}
			}
		}
		return errors.New("TLS client certificate is not an authorized Raft voter")
	}
	_, listenPort, err := net.SplitHostPort(cfg.Listen)
	port, portErr := strconv.Atoi(listenPort)
	if err != nil || portErr != nil || port < 1 || port > 65535 {
		return nil, errors.New("Raft listen address must have a fixed TCP port between 1 and 65535")
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("listen for Raft peers: %w", err)
	}
	stream := &peerStream{
		Listener: listener, advertised: peerAddr(local.Address), clientTLS: clientTLS, serverTLS: serverTLS,
		targets: make(map[raft.ServerAddress]Peer, len(peers)), incoming: make(map[*peerConn]struct{}),
	}
	for _, peer := range peers {
		if peer.ID != local.ID {
			stream.targets[raft.ServerAddress(peer.Address)] = peer
		}
	}
	return stream, nil
}

func verifyLocalCert(certificate tls.Certificate, roots *x509.CertPool, local Peer) error {
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return err
	}
	if err := verifyPeerCert(leaf, local); err != nil {
		return err
	}
	intermediates := x509.NewCertPool()
	for _, data := range certificate.Certificate[1:] {
		cert, err := x509.ParseCertificate(data)
		if err != nil {
			return err
		}
		intermediates.AddCert(cert)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage},
		}); err != nil {
			return err
		}
	}
	return nil
}

func verifyPeerCert(leaf *x509.Certificate, peer Peer) error {
	want := peerURI + peer.ID
	var identity string
	for _, uri := range leaf.URIs {
		if name := uri.String(); strings.HasPrefix(name, peerURI) {
			if identity != "" {
				return errors.New("TLS certificate contains multiple Raft identities")
			}
			identity = name
		}
	}
	if identity != want {
		return fmt.Errorf("TLS certificate needs URI SAN %s", want)
	}
	host, _, _ := net.SplitHostPort(peer.Address)
	return leaf.VerifyHostname(host)
}

func (s *peerStream) Addr() net.Addr { return s.advertised }

func (s *peerStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	peer, ok := s.targets[address]
	if !ok {
		return nil, fmt.Errorf("Raft target %q is outside the configured roster", address)
	}
	config := s.clientTLS.Clone()
	config.ServerName, _, _ = net.SplitHostPort(peer.Address)
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if state.NegotiatedProtocol != config.NextProtos[0] {
			return errors.New("Raft peer roster or policy does not match")
		}
		return verifyPeerCert(state.PeerCertificates[0], peer)
	}
	return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", peer.Address, config)
}

func (s *peerStream) Accept() (net.Conn, error) {
	raw, err := s.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if err := raw.SetDeadline(time.Now().Add(transportTimeout)); err != nil {
		raw.Close()
		return nil, err
	}
	conn := &peerConn{Conn: tls.Server(raw, s.serverTLS), owner: s}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.incoming == nil || len(s.incoming) >= maxIncoming {
		raw.Close()
		return nil, errors.New("Raft peer listener cannot accept another connection")
	}
	s.incoming[conn] = struct{}{}
	return conn, nil
}

func (s *peerStream) Close() error {
	err := s.Listener.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn := range s.incoming {
		conn.NetConn().Close()
	}
	s.incoming = nil
	return err
}

type peerConn struct {
	*tls.Conn
	owner        *peerStream
	handshake    sync.Once
	handshakeErr error
}

func (c *peerConn) Read(buf []byte) (int, error) {
	// Raft reads in per-connection goroutines. Complete the bounded handshake
	// there to keep the listener responsive to other voters.
	c.handshake.Do(func() {
		c.handshakeErr = c.Handshake()
		if c.handshakeErr == nil {
			c.handshakeErr = c.SetDeadline(time.Time{})
		}
	})
	if c.handshakeErr != nil {
		return 0, c.handshakeErr
	}
	return c.Conn.Read(buf)
}

func (c *peerConn) Close() error {
	c.owner.mu.Lock()
	delete(c.owner.incoming, c)
	c.owner.mu.Unlock()
	return c.Conn.Close()
}
