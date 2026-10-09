package directory

import (
	"context"
	"sync"

	"github.com/go-ldap/ldap/v3"
)

// contextConnection closes the socket when the owning operation is cancelled.
// go-ldap bounds requests with SetTimeout but Bind and Search do not accept a
// context; closing their dedicated connection interrupts an in-flight request.
type contextConnection struct {
	ctx        context.Context
	conn       ldapConnection
	closeOnce  sync.Once
	stopCancel func() bool
}

func newContextConnection(ctx context.Context, conn ldapConnection) *contextConnection {
	wrapped := &contextConnection{ctx: ctx, conn: conn}
	wrapped.stopCancel = context.AfterFunc(ctx, wrapped.closeTransport)
	return wrapped
}

func (c *contextConnection) Bind(username, password string) error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	return contextOrError(c.ctx, c.conn.Bind(username, password))
}

func (c *contextConnection) Search(request *ldap.SearchRequest) (*ldap.SearchResult, error) {
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	result, err := c.conn.Search(request)
	if err = contextOrError(c.ctx, err); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *contextConnection) closeTransport() {
	c.closeOnce.Do(c.conn.Close)
}

func (c *contextConnection) Close() {
	c.stopCancel()
	c.closeTransport()
}
