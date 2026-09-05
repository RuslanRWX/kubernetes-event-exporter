package sinks

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
)

// Sink is the interface that the third-party providers should implement. It should just get the event and
// transform it depending on its configuration and submit it. Error handling for retries etc. should be handled inside
// for now.
type Sink interface {
	Send(ctx context.Context, ev *kube.EnhancedEvent) error
	Close()
}

// BatchSink is an extension Sink that can handle batch events.
// NOTE: Currently no provider implements it nor the receivers can handle it.
type BatchSink interface {
	Sink
	SendBatch([]*kube.EnhancedEvent) error
}

type TLS struct {
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
	ServerName         string `yaml:"serverName"`
	CaFile             string `yaml:"caFile"`
	KeyFile            string `yaml:"keyFile"`
	CertFile           string `yaml:"certFile"`
}

func setupTLS(cfg *TLS) (*tls.Config, error) {
	tlsClientConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		ServerName:         cfg.ServerName,
	}

	if len(cfg.CaFile) > 0 {
		readFile, err := os.ReadFile(cfg.CaFile)
		if err != nil {
			return nil, err
		}

		tlsClientConfig.RootCAs = x509.NewCertPool()
		tlsClientConfig.RootCAs.AppendCertsFromPEM(readFile)
	}

	if len(cfg.KeyFile) > 0 && len(cfg.CertFile) > 0 {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("could not read client certificate or key: %w", err)
		}
		tlsClientConfig.Certificates = append(tlsClientConfig.Certificates, cert)
	}
	if len(cfg.KeyFile) > 0 && len(cfg.CertFile) == 0 {
		return nil, errors.New("configured keyFile but forget certFile for client certificate authentication")
	}
	if len(cfg.KeyFile) == 0 && len(cfg.CertFile) > 0 {
		return nil, errors.New("configured certFile but forget keyFile for client certificate authentication")
	}
	return tlsClientConfig, nil
}

// newHTTPTransport builds a transport for the HTTP-based sinks. It is cloned from
// http.DefaultTransport so that proxy settings, dial/handshake timeouts and
// HTTP/2 negotiation are inherited; a bare &http.Transport{} has none of those
// and caps idle connections per host at 2, which forces a fresh TCP (and TLS)
// handshake for most events.
func newHTTPTransport(tlsClientConfig *tls.Config) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsClientConfig
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 32
	transport.IdleConnTimeout = 90 * time.Second
	transport.ResponseHeaderTimeout = 30 * time.Second
	return transport
}

// maxDrainBytes bounds how much of an unread response body we are willing to
// consume before giving up on reusing the connection.
const maxDrainBytes = 2 << 20

// drainAndCloseBody consumes the remainder of an HTTP response body and closes
// it. Without draining, net/http cannot return the connection to the idle pool.
func drainAndCloseBody(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxDrainBytes))
	_ = body.Close()
}

// handleIndexResponse turns a non-2xx/3xx indexing response into an error so the
// caller counts it in send_event_errors and surfaces it. Previously such
// responses were only logged and the send reported success, so rejected
// documents (mapping conflicts, a read-only index, auth failures) were invisible.
func handleIndexResponse(statusCode int, body io.ReadCloser, index string) error {
	defer drainAndCloseBody(body)

	if statusCode < 400 {
		return nil
	}

	rb, err := io.ReadAll(io.LimitReader(body, 64<<10))
	if err != nil {
		return fmt.Errorf("indexing into %q failed with status %d (response body unreadable: %w)", index, statusCode, err)
	}
	return fmt.Errorf("indexing into %q failed with status %d: %s", index, statusCode, strings.TrimSpace(string(rb)))
}
