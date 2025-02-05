package pgctx_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"

	"github.com/xkamail/pgsql"
	"github.com/xkamail/pgsql/pgctx"
)

func newCtx(t *testing.T) (context.Context, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	assert.NoError(t, err)
	return pgctx.NewContext(context.Background(), mock), mock
}

func TestNewContext(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() {
		db, err := pgxmock.NewPool()
		assert.NoError(t, err)
		pgctx.NewContext(context.Background(), db)
	})
}

type testKey1 struct{}

func TestNewKeyContext(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() {
		mock, err := pgxmock.NewPool()
		assert.NoError(t, err)
		ctx := pgctx.NewKeyContext(context.Background(), testKey1{}, mock)
		assert.NotNil(t, ctx)

	})
}

func TestMiddleware(t *testing.T) {
	t.Parallel()
	mock, err := pgxmock.NewPool()
	assert.NoError(t, err)

	called := false
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	pgctx.Middleware(mock)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		ctx := r.Context()
		assert.NotPanics(t, func() {
			pgctx.QueryRow(ctx, "select 1")
		})
		assert.NotPanics(t, func() {
			pgctx.Query(ctx, "select 1")
		})
		assert.NotPanics(t, func() {
			pgctx.Exec(ctx, "select 1")
		})
	})).ServeHTTP(w, r)
	assert.True(t, called)

}

func TestKeyMiddleware(t *testing.T) {
	t.Parallel()

	db, err := pgxmock.NewPool()
	assert.NoError(t, err)

	called := false
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	pgctx.KeyMiddleware(testKey1{}, db)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		ctx := r.Context()
		assert.NotPanics(t, func() {
			pgctx.QueryRow(pgctx.With(ctx, testKey1{}), "select 1")
		})
		assert.NotPanics(t, func() {
			pgctx.Query(pgctx.With(ctx, testKey1{}), "select 1")
		})
		assert.NotPanics(t, func() {
			pgctx.Exec(pgctx.With(ctx, testKey1{}), "select 1")
		})
		assert.Panics(t, func() {
			pgctx.QueryRow(ctx, "select 1")
		})
	})).ServeHTTP(w, r)
	assert.True(t, called)
}

func TestRunTx(t *testing.T) {
	t.Parallel()
	type Result struct {
		a int
	}
	t.Run("Committed", func(t *testing.T) {
		ctx, mock := newCtx(t)

		called := false
		mock.ExpectBegin()
		mock.ExpectCommit()
		result, err := pgctx.RunTx(ctx, func(ctx context.Context) (*Result, error) {
			called = true
			return &Result{a: 1}, nil
		})
		assert.NotNil(t, result)
		assert.Equal(t, result.a, 1)
		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("Rollback with error", func(t *testing.T) {
		ctx, mock := newCtx(t)

		mock.ExpectBegin()
		mock.ExpectRollback()
		var retErr = fmt.Errorf("error")
		result, err := pgctx.RunTx(ctx, func(ctx context.Context) (*Result, error) {
			return nil, retErr
		})
		assert.Nil(t, result)
		assert.Error(t, err)
		assert.Equal(t, retErr, err)
	})

	t.Run("Abort Tx", func(t *testing.T) {
		ctx, mock := newCtx(t)

		mock.ExpectBegin()
		mock.ExpectCommit()
		result, err := pgctx.RunTx(ctx, func(ctx context.Context) (*Result, error) {
			return nil, pgsql.ErrAbortTx
		})
		assert.Nil(t, result)
		assert.NoError(t, err)
	})

	t.Run("Nested Tx", func(t *testing.T) {
		ctx, mock := newCtx(t)

		mock.ExpectBegin()
		mock.ExpectCommit()
		result, err := pgctx.RunTx(ctx, func(ctx context.Context) (*Result, error) {
			return pgctx.RunTx(ctx, func(ctx context.Context) (*Result, error) {
				return &Result{a: 2}, nil
			})
		})
		assert.NotNil(t, result)
		assert.Equal(t, result.a, 2)
		assert.NoError(t, err)
	})
}

func TestRunInTx(t *testing.T) {
	t.Parallel()

	t.Run("Committed", func(t *testing.T) {
		ctx, mock := newCtx(t)

		called := false
		mock.ExpectBegin()
		mock.ExpectCommit()
		err := pgctx.RunInTx(ctx, func(ctx context.Context) error {
			called = true
			return nil
		})
		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("Rollback with error", func(t *testing.T) {
		ctx, mock := newCtx(t)

		mock.ExpectBegin()
		mock.ExpectRollback()
		var retErr = fmt.Errorf("error")
		err := pgctx.RunInTx(ctx, func(ctx context.Context) error {
			return retErr
		})
		assert.Error(t, err)
		assert.Equal(t, retErr, err)
	})

	t.Run("Abort Tx", func(t *testing.T) {
		ctx, mock := newCtx(t)

		mock.ExpectBegin()
		mock.ExpectCommit()
		err := pgctx.RunInTx(ctx, func(ctx context.Context) error {
			return pgsql.ErrAbortTx
		})
		assert.NoError(t, err)
	})

	t.Run("Nested Tx", func(t *testing.T) {
		ctx, mock := newCtx(t)

		mock.ExpectBegin()
		mock.ExpectCommit()
		err := pgctx.RunInTx(ctx, func(ctx context.Context) error {
			return pgctx.RunInTx(ctx, func(ctx context.Context) error {
				return nil
			})
		})
		assert.NoError(t, err)
	})
}

func TestCommitted(t *testing.T) {
	t.Parallel()

	t.Run("Outside Tx", func(t *testing.T) {
		ctx, _ := newCtx(t)
		var called bool
		pgctx.Committed(ctx, func(ctx context.Context) {
			called = true
		})
		assert.True(t, called)
	})

	t.Run("Nil func", func(t *testing.T) {
		ctx, mock := newCtx(t)

		mock.ExpectBegin()
		mock.ExpectCommit()
		pgctx.RunInTx(ctx, func(ctx context.Context) error {
			pgctx.Committed(ctx, nil)
			return nil
		})
	})

	t.Run("Committed", func(t *testing.T) {
		ctx, mock := newCtx(t)

		called := false
		mock.ExpectBegin()
		mock.ExpectCommit()
		err := pgctx.RunInTx(ctx, func(ctx context.Context) error {
			pgctx.Committed(ctx, func(ctx context.Context) {
				called = true
			})
			return nil
		})
		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("Rollback", func(t *testing.T) {
		ctx, mock := newCtx(t)

		mock.ExpectBegin()
		mock.ExpectRollback()
		err := pgctx.RunInTx(ctx, func(ctx context.Context) error {
			pgctx.Committed(ctx, func(ctx context.Context) {
				assert.Fail(t, "should not be called")
			})
			return pgsql.ErrAbortTx
		})
		assert.NoError(t, err)
	})

}
