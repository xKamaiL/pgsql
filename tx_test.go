package pgsql_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xkamail/pgsql"
)

func TestTx(t *testing.T) {
	db := open(t)
	_, err := db.Exec(context.Background(), `
		drop table if exists test_pgsql_tx;
		create table test_pgsql_tx (
			id int primary key,
			value int
		);
		insert into test_pgsql_tx (
			id, value
		) values
			(0, 0),
			(1, 0),
			(2, 0);
	`)
	if err != nil {
		t.Fatalf("prepare table error; %v", err)
	}
	defer db.Exec(context.Background(), `drop table test_pgsql_tx`)
	//db.SetMaxOpenConns(30)
	db.Config().MaxConns = 30

	opts := &pgsql.TxOptions{
		TxOptions: pgx.TxOptions{
			IsoLevel: pgx.Serializable,
		},
		MaxAttempts: 10,
	}

	deposit := func(balance int) error {
		return pgsql.RunInTx(db, opts, func(tx pgx.Tx) error {
			var err error
			ctx := context.Background()

			// log.Println("deposit", balance)
			var acc0, acc1 int
			err = tx.QueryRow(ctx, `select value from test_pgsql_tx where id = 0`).Scan(&acc0)
			if err != nil {
				return err
			}
			err = tx.QueryRow(ctx, `select value from test_pgsql_tx where id = 1`).Scan(&acc1)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `update test_pgsql_tx set value = $1 where id = 0`, acc0-balance)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `update test_pgsql_tx set value = $1 where id = 1`, acc1+balance)
			if err != nil {
				return err
			}
			return nil
		})
	}
	withdraw := func(balance int) error {
		return pgsql.RunInTx(db, opts, func(tx pgx.Tx) error {
			var err error
			ctx := context.Background()
			// log.Println("withdraw", balance)
			var acc0, acc1 int
			err = tx.QueryRow(ctx, `select value from test_pgsql_tx where id = 1`).Scan(&acc1)
			if err != nil {
				return err
			}
			if acc1 < balance {
				return fmt.Errorf("not enough balance to withdraw")
			}
			err = tx.QueryRow(ctx, `select value from test_pgsql_tx where id = 0`).Scan(&acc0)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `update test_pgsql_tx set value = $1 where id = 0`, acc0+balance)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `update test_pgsql_tx set value = $1 where id = 1`, acc1-balance)
			if err != nil {
				return err
			}
			return nil
		})
	}
	transfer := func(balance int) error {
		return pgsql.RunInTx(db, opts, func(tx pgx.Tx) error {
			var err error
			ctx := context.Background()

			// log.Println("transfer", balance)
			var acc1, acc2 int
			err = tx.QueryRow(ctx, `select value from test_pgsql_tx where id = 1`).Scan(&acc1)
			if err != nil {
				return err
			}
			if acc1 < balance {
				return fmt.Errorf("not enough balance to transfer")
			}
			err = tx.QueryRow(ctx, `select value from test_pgsql_tx where id = 2`).Scan(&acc2)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `update test_pgsql_tx set value = $1 where id = 1`, acc1-balance)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `update test_pgsql_tx set value = $1 where id = 2`, acc2+balance)
			if err != nil {
				return err
			}
			return nil
		})
	}

	wg := sync.WaitGroup{}
	rand.Seed(time.Now().Unix())
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			var err error
			k := rand.Intn(3)
			if k == 0 {
				err = deposit(rand.Intn(100000))
			} else if k == 1 {
				err = withdraw(rand.Intn(100000))
			} else {
				err = transfer(rand.Intn(100000))
			}
			if err != nil {
				log.Println(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	var result int
	err = db.QueryRow(context.Background(), `select sum(value) from test_pgsql_tx`).Scan(&result)
	if err != nil {
		t.Fatalf("query result error; %v", err)
	}
	if result != 0 {
		t.Fatalf("expected sum all value to be 0; got %d", result)
	}
}
