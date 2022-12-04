package datasource

import (
	"database/sql"
	"fmt"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/zeromicro/go-zero/core/logx"

	"time"
)

type ConfigDBSQL struct {
	Driver           string            `json:",optional"`
	UserName         string            `json:",optional"`
	Password         string            `json:",optional"`
	IP               string            `json:",optional"`
	Port             int               `json:",optional"`
	DefaultSchema    string            `json:",optional"`
	MaxIdleTime      int               `json:",optional"`
	MaxLifeTime      int               `json:",optional"`
	MaxIdle          int               `json:",optional"`
	MaxOpen          int               `json:",optional"`
	ConnectionString string            `json:",optional"`
	SslEnabled       bool              `json:",optional"`
	Options          map[string]string `json:",optional"`
}

type ConfigMongoDB struct {
	URI      string `json:",optional"`
	Database string `json:",optional"`
}

type DBType interface {
	*sql.DB
}

type DBManager[T DBType] interface {
	GetConnection() T
	Close()
}

type DB[T DBType] struct {
	DB T
}

// GetConnection implements DBManager
func (db *DB[T]) GetConnection() T {
	return db.DB
}

// GetConnection implements DBManager
func (db *DB[T]) Close() {
	switch db := any(db.DB).(type) {
	case *sql.DB:
		db.Close()
	}
}

func InitDB[T *sql.DB](cfg ConfigDBSQL) (DBManager[T], error) {
	logx.Info(fmt.Sprintf("Connecting to %s:%d, use driver: %s", cfg.IP, cfg.Port, cfg.Driver))
	var connStr, queryVersion string

	switch cfg.Driver {
	case "oracle":
		connStr = go_ora.BuildJDBC(cfg.UserName, cfg.Password, cfg.ConnectionString, cfg.Options)
		queryVersion = "SELECT Banner FROM v$version"
		break
	case "mysql":
		connStr = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.UserName, cfg.Password,
			cfg.IP, cfg.Port, cfg.DefaultSchema)
		queryVersion = "SELECT VERSION()"
		break
	case "postgres":
		sslmode := "disable"
		if cfg.SslEnabled {
			sslmode = "enable"
		}
		connStr = fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=%s",
			cfg.IP, cfg.Port, cfg.UserName, cfg.Password, cfg.DefaultSchema, sslmode)
		queryVersion = "SELECT VERSION()"
		break
	case "sqlite3":
		connStr = cfg.ConnectionString
		queryVersion = "SELECT sqlite_version()"
		break
	}
	conn, err := sql.Open(cfg.Driver, connStr)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	conn.SetConnMaxIdleTime(time.Duration(cfg.MaxIdleTime) * time.Second)
	conn.SetConnMaxLifetime(time.Duration(cfg.MaxLifeTime) * time.Second)
	conn.SetMaxIdleConns(cfg.MaxIdle)
	conn.SetMaxOpenConns(cfg.MaxOpen)

	var version string
	if err := conn.QueryRow(queryVersion).Scan(&version); err != nil {
		logx.Error(err)
		return nil, err
	}
	logx.Info(fmt.Sprintf("Connected! Use driver %s. Database version: %s", cfg.Driver, version))
	return &DB[T]{
		DB: conn,
	}, nil
}
