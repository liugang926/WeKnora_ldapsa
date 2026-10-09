package directory

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/stretchr/testify/require"
)

type blockingLDAPConnection struct {
	entered    chan struct{}
	closed     chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func (c *blockingLDAPConnection) Bind(string, string) error {
	close(c.entered)
	<-c.closed
	return errors.New("connection closed")
}

func (c *blockingLDAPConnection) Search(*ldap.SearchRequest) (*ldap.SearchResult, error) {
	return nil, c.Bind("", "")
}

func (c *blockingLDAPConnection) Close() {
	c.closeCalls.Add(1)
	c.closeOnce.Do(func() { close(c.closed) })
}

func TestContextConnectionInterruptsInFlightRequests(t *testing.T) {
	for _, operation := range []string{"bind", "search"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			transport := &blockingLDAPConnection{entered: make(chan struct{}), closed: make(chan struct{})}
			conn := newContextConnection(ctx, transport)
			defer conn.Close()
			completed := make(chan error, 1)
			go func() {
				if operation == "bind" {
					completed <- conn.Bind("user", "password")
					return
				}
				_, err := conn.Search(&ldap.SearchRequest{})
				completed <- err
			}()
			<-transport.entered
			cancel()
			select {
			case err := <-completed:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("cancelled LDAP request did not unblock")
			}
			conn.Close()
			require.EqualValues(t, 1, transport.closeCalls.Load(), "transport closes once across cancellation/cleanup")
			require.ErrorIs(t, conn.Bind("user", "password"), context.Canceled)
		})
	}
}

func TestConnectionNegotiationHonorsContextCancellation(t *testing.T) {
	for _, mode := range []TLSMode{TLSModeLDAPS, TLSModeStartTLS} {
		t.Run(string(mode), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, acceptErr := listener.Accept()
				if acceptErr == nil {
					accepted <- conn
				}
			}()
			scheme := "ldap"
			if mode == TLSModeLDAPS {
				scheme = "ldaps"
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			completed := make(chan error, 1)
			go func() {
				conn, dialErr := defaultDialConnection(ctx,
					Controller{URL: scheme + "://" + listener.Addr().String(), TLSMode: mode},
					&tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS12},
					Config{ConnectTimeout: 10 * time.Second, QueryTimeout: 10 * time.Second},
				)
				if conn != nil {
					conn.Close()
				}
				completed <- dialErr
			}()
			select {
			case socket := <-accepted:
				defer socket.Close()
			case <-time.After(time.Second):
				t.Fatal("test server did not accept connection")
			}
			cancel()
			select {
			case err := <-completed:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("cancelled TLS negotiation did not unblock")
			}
		})
	}
}
