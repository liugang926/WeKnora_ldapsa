package directory

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ber "github.com/go-asn1-ber/asn1-ber"
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

type observedWriteConnection struct {
	net.Conn
	writing chan struct{}
	once    sync.Once
}

func (c *observedWriteConnection) Write(bytes []byte) (int, error) {
	c.once.Do(func() { close(c.writing) })
	return c.Conn.Write(bytes)
}

func TestProductionConnectionCancellationInterruptsBlockedSocketWrite(t *testing.T) {
	for _, operation := range []string{"bind", "search"} {
		t.Run(operation, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			transport := &observedWriteConnection{Conn: client, writing: make(chan struct{})}
			ldapConn := ldap.NewConn(transport, false)
			ldapConn.Start()
			ldapConn.SetTimeout(10 * time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			conn := newContextConnection(ctx, &productionConnection{conn: ldapConn, socket: transport})
			defer conn.Close()
			completed := make(chan error, 1)
			go func() {
				if operation == "bind" {
					completed <- conn.Bind("user", "password")
					return
				}
				_, err := conn.Search(ldap.NewSearchRequest("DC=example,DC=test", ldap.ScopeBaseObject,
					ldap.NeverDerefAliases, 1, 1, false, "(objectClass=*)", []string{"dn"}, nil))
				completed <- err
			}()
			<-transport.writing
			cancel()
			select {
			case err := <-completed:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("socket write cancellation waited for LDAP request timeout")
			}
		})
	}
}

func TestProductionQueryDeadlineBoundsBlockedSocketWrite(t *testing.T) {
	for _, operation := range []string{"bind", "search"} {
		t.Run(operation, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			ldapConn := ldap.NewConn(client, false)
			ldapConn.Start()
			ldapConn.SetTimeout(10 * time.Second)
			conn := &productionConnection{conn: ldapConn, socket: client, queryTimeout: 75 * time.Millisecond}
			defer conn.Close()
			completed := make(chan error, 1)
			go func() {
				if operation == "bind" {
					completed <- conn.Bind("user", "password")
					return
				}
				_, err := conn.Search(ldap.NewSearchRequest("DC=example,DC=test", ldap.ScopeBaseObject,
					ldap.NeverDerefAliases, 1, 1, false, "(objectClass=*)", []string{"dn"}, nil))
				completed <- err
			}()
			select {
			case err := <-completed:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("blocked socket write ignored configured query timeout")
			}
		})
	}
}

func TestProductionQueryDeadlineHonorsEarlierContextDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	ldapConn := ldap.NewConn(client, false)
	ldapConn.Start()
	ldapConn.SetTimeout(10 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	conn := &productionConnection{conn: ldapConn, socket: client, queryTimeout: 10 * time.Second, ctx: ctx}
	defer conn.Close()
	completed := make(chan error, 1)
	go func() { completed <- conn.Bind("user", "password") }()
	select {
	case err := <-completed:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("query ignored earlier request context deadline")
	}
}

func TestStartTLSCancellationInterruptsHandshakeAfterSuccessfulExtendedResponse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	responseSent := make(chan net.Conn, 1)
	serverErrors := make(chan error, 1)
	go func() {
		socket, err := listener.Accept()
		if err != nil {
			serverErrors <- err
			return
		}
		request, err := ber.ReadPacket(socket)
		if err != nil {
			socket.Close()
			serverErrors <- err
			return
		}
		response := ber.Encode(ber.ClassUniversal, ber.TypeConstructed, ber.TagSequence, nil, "LDAP response")
		response.AppendChild(request.Children[0])
		extended := ber.Encode(ber.ClassApplication, ber.TypeConstructed, ldap.ApplicationExtendedResponse, nil, "StartTLS response")
		extended.AppendChild(ber.NewInteger(ber.ClassUniversal, ber.TypePrimitive, ber.TagEnumerated, int64(0), "success"))
		extended.AppendChild(ber.NewString(ber.ClassUniversal, ber.TypePrimitive, ber.TagOctetString, "", "matched DN"))
		extended.AppendChild(ber.NewString(ber.ClassUniversal, ber.TypePrimitive, ber.TagOctetString, "", "diagnostic"))
		response.AppendChild(extended)
		if _, err := socket.Write(response.Bytes()); err != nil {
			socket.Close()
			serverErrors <- err
			return
		}
		// Wait for the ClientHello record to prove the extended operation has
		// completed and the client is blocked inside the TLS handshake itself.
		header := make([]byte, 5)
		if _, err := io.ReadFull(socket, header); err != nil {
			socket.Close()
			serverErrors <- err
			return
		}
		if header[0] != 22 {
			socket.Close()
			serverErrors <- errors.New("expected TLS ClientHello after StartTLS response")
			return
		}
		responseSent <- socket
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		conn, err := defaultDialConnection(ctx,
			Controller{URL: "ldap://" + listener.Addr().String(), TLSMode: TLSModeStartTLS},
			&tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS12},
			Config{ConnectTimeout: 10 * time.Second, QueryTimeout: 10 * time.Second})
		if conn != nil {
			conn.Close()
		}
		completed <- err
	}()
	select {
	case socket := <-responseSent:
		defer socket.Close()
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("test server did not send successful StartTLS response")
	}
	cancel()
	select {
	case err := <-completed:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("StartTLS TLS handshake cancellation did not unblock")
	}
}
