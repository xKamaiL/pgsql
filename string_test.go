package pgsql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/acoshift/pgsql"
)

func TestNullString(t *testing.T) {
	db := open(t)

	_, err := db.Exec(context.Background(), `
		drop table if exists test_pgsql_null_string;
		create table test_pgsql_null_string (
			id int primary key,
			value varchar
		);
		insert into test_pgsql_null_string (
			id, value
		) values
			(0, 'hello'),
			(1, null);
	`)
	require.NoError(t, err)
	defer db.Exec(context.Background(), `drop table test_pgsql_null_string`)

	t.Run("Scan", func(t *testing.T) {
		{
			var p string
			err = db.QueryRow(context.Background(), `select value from test_pgsql_null_string where id = 0`).Scan(pgsql.NullString(&p))
			require.NoError(t, err)
			assert.Equal(t, "hello", p)
		}

		{
			var p string
			err = db.QueryRow(context.Background(), `select value from test_pgsql_null_string where id = 1`).Scan(pgsql.NullString(&p))
			require.NoError(t, err)
			assert.Equal(t, "", p)
		}
	})
}
