package dbdriver

import (
	"database/sql"
	"sync"

	"github.com/jmoiron/sqlx"
)

func DBConnect(driver string, db *sql.DB) *DB {
	var engine Driver
	var locker Locker
	switch driver {
	case "mysql":
		engine = Mysql
		locker = &nopLocker{}
	case "postgres":
		engine = Postgres
		locker = &nopLocker{}
	case "oracle":
		engine = Oracle
		locker = &nopLocker{}
	default:
		engine = Sqlite
		locker = &sync.RWMutex{}
	}
	return &DB{
		conn:   sqlx.NewDb(db, driver),
		lock:   locker,
		driver: engine,
	}
}
