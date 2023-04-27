package pgsql_test

import (
	"context"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"

	"github.com/xkamail/pgsql"
)

func TestIsUniqueViolation(t *testing.T) {
	t.Parallel()

	assert.True(t, pgsql.IsUniqueViolation(&pq.Error{
		Severity:   "Error",
		Code:       "23505",
		Message:    "",
		Table:      "users",
		Constraint: "users_email_key",
	}))

	assert.True(t, pgsql.IsUniqueViolation(&pq.Error{
		Severity:   "Error",
		Code:       "23505",
		Message:    "",
		Table:      "users",
		Constraint: "users_email_key",
	}, "pkey", "users_email_key"))

	assert.False(t, pgsql.IsUniqueViolation(&pq.Error{
		Severity:   "Error",
		Code:       "23505",
		Message:    "",
		Table:      "users",
		Constraint: "users_email_key",
	}, "pkey"))

	assert.False(t, pgsql.IsUniqueViolation(&pq.Error{
		Severity:   "Error",
		Code:       "23503",
		Message:    "",
		Table:      "users",
		Constraint: "users_email_key",
	}))
}

func TestIsForeignKeyViolation(t *testing.T) {
	t.Parallel()

	assert.True(t, pgsql.IsForeignKeyViolation(&pq.Error{
		Severity:   "Error",
		Code:       "23503",
		Message:    "",
		Table:      "b",
		Constraint: "b_a_id_fkey",
	}))

	assert.True(t, pgsql.IsForeignKeyViolation(&pq.Error{
		Severity:   "Error",
		Code:       "23503",
		Message:    "",
		Table:      "b",
		Constraint: "b_a_id_fkey",
	}, "pkey", "b_a_id_fkey"))

	assert.True(t, pgsql.IsForeignKeyViolation(&pq.Error{
		Severity:   "Error",
		Code:       "23503",
		Message:    `insert or update on table "b" violates foreign key constraint "b_a_id_fkey"`,
		Table:      "b",
		Constraint: "",
	}, "pkey", "b_a_id_fkey"))

	assert.True(t, pgsql.IsForeignKeyViolation(&pq.Error{
		Severity:   "Error",
		Code:       "23503",
		Message:    `foreign key violation: value ['b'] not found in a@primary [id] (txn=e3f9af56-5f73-4899-975c-4bb1de800402)`,
		Table:      "b",
		Constraint: "",
	}, "pkey", "b_a_id_fkey", "a@primary"))
}

func TestIsQueryCanceled(t *testing.T) {
	t.Parallel()

	db := open(t)
	defer db.Close()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	time.AfterFunc(100*time.Millisecond, cancel)
	_, err := db.Exec(ctx, "select pg_sleep(1)")

	assert.True(t, pgsql.IsQueryCanceled(err))
}
