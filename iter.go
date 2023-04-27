package pgsql

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5"
)

type Scanner func(dest ...any) error

type Iterator func(scan Scanner) error

// QueryContext interface
type QueryContext interface {
	Query(context.Context, string, ...any) (*sql.Rows, error)
}

func Iter(q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, iter Iterator, query string, args ...any) error {
	return IterContext(context.Background(), q, iter, query, args...)
}

func IterContext(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, iter Iterator, query string, args ...any) error {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		err := iter(rows.Scan)
		if err != nil {
			return err
		}
	}

	return rows.Err()
}
