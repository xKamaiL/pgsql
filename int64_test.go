package pgsql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xkamail/pgsql"
)

func TestNullInt64(t *testing.T) {
	db := open(t)

	_, err := db.Exec(context.Background(), `
		drop table if exists test_pgsql_null_int64;
		create table test_pgsql_null_int64 (
			id int primary key,
			value int
		);
		insert into test_pgsql_null_int64 (
			id, value
		) values
			(0, 1),
			(1, null);
	`)
	if !assert.NoError(t, err) {
		return
	}
	defer db.Exec(`drop table test_pgsql_null_int64`)

	t.Run("Scan", func(t *testing.T) {
		{
			ctx := context.Background()
			var p int64
			err = db.QueryRow(`select value from test_pgsql_null_int64 where id = 0`).Scan(pgsql.NullInt64(&p))
			assert.NoError(t, err)
			assert.Equal(t, int64(1), p)
		}

		{
			ctx := context.Background()
			var p int64
			err = db.QueryRow(`select value from test_pgsql_null_int64 where id = 1`).Scan(pgsql.NullInt64(&p))
			assert.NoError(t, err)
			assert.Equal(t, int64(0), p)
		}
	})
}
