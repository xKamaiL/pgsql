package pgsql_test

import (
	"context"
	"testing"
	"time"

	"github.com/xkamail/pgsql"
)

func TestTime(t *testing.T) {
	db := open(t)

	_, err := db.Exec(context.Background(), `
		drop table if exists test_pgsql_time;
		create table test_pgsql_time (
			id int primary key,
			value timestamp
		);
		insert into test_pgsql_time (
			id, value
		) values
			(0, now()),
			(1, null);
	`)
	if err != nil {
		t.Fatalf("prepare table error; %v", err)
	}
	defer db.Exec(context.Background(), `drop table test_pgsql_time`)

	var n, k time.Time
	var p pgsql.Time
	err = db.QueryRow(context.Background(), `select value from test_pgsql_time where id = 0`).Scan(&p)
	if err != nil {
		t.Fatalf("scan time error; %v", err)
	}
	err = db.QueryRow(context.Background(), `select value from test_pgsql_time where id = 0`).Scan(&n)
	if err != nil {
		t.Fatalf("scan native time error; %v", err)
	}
	if !p.Equal(n) {
		t.Fatalf("scan time not equal when insert; expected %v; got %v", n, p)
	}
	err = db.QueryRow(context.Background(), `select value from test_pgsql_time where id = 0`).Scan(pgsql.NullTime(&k))
	if err != nil {
		t.Fatalf("scan null time error; %v", err)
	}
	if !k.Equal(n) {
		t.Fatalf("scan time not equal when insert; expected %v; got %v", n, p)
	}

	err = db.QueryRow(context.Background(), `select value from test_pgsql_time where id = 1`).Scan(&p)
	if err != nil {
		t.Fatalf("scan time error; %v", err)
	}
	if !p.IsZero() {
		t.Fatalf("invalid time; expected empty got %v", p)
	}

	n = time.Now()
	p.Time = n
	var ok bool
	err = db.QueryRow(context.Background(), `select $1 = $2`, p, n).Scan(&ok)
	if err != nil {
		t.Fatalf("sql error; %v", err)
	}
	if !ok {
		t.Fatalf("invalid time")
	}

	err = db.QueryRow(context.Background(), `select $1 = $2`, pgsql.NullTime(&n), n).Scan(&ok)
	if err != nil {
		t.Fatalf("sql error; %v", err)
	}
	if !ok {
		t.Fatalf("invalid time")
	}

	p.Time = time.Time{}
	err = db.QueryRow(context.Background(), `insert into test_pgsql_time (id, value) values (2, $1) returning value is null`, p).Scan(&ok)
	if err != nil {
		t.Fatalf("sql error; %v", err)
	}
	if !ok {
		t.Fatalf("invalid time")
	}

	err = db.QueryRow(context.Background(), `insert into test_pgsql_time (id, value) values (3, $1) returning value is null`, pgsql.NullTime(new(time.Time))).Scan(&ok)
	if err != nil {
		t.Fatalf("sql error; %v", err)
	}
	if !ok {
		t.Fatalf("invalid time")
	}
}
