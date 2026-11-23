package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

func serializationAborted(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

// Retry only a complete database transaction. A nested GORM transaction is a
// savepoint and cannot obtain a fresh PostgreSQL snapshot.
func retryKnowledgeTagWrite(ctx context.Context, db *gorm.DB, write func() error) error {
	if _, nested := db.Statement.ConnPool.(gorm.TxCommitter); nested || db.Name() != "postgres" {
		return write()
	}
	const maxAttempts = 5
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := write()
		if attempt == maxAttempts || !serializationAborted(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After((10 * time.Millisecond) << (attempt - 1)):
		}
	}
}
