package main

import (
	"errors"
	"reflect"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/xwb1989/sqlparser"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// SessionHandler - a session handler
type SessionHandler struct {
	store     *Store
	db        *sql.DB
	currentdb string
}

// NewSessionHandler - initialize a new handler
func NewSessionHandler(s *Store) (*SessionHandler, error) {
	h := new(SessionHandler)
	h.store = s

	h.UseDB("__default__")

	return h, nil
}

// UseDB - implements the `server.Handler` interface
func (h *SessionHandler) UseDB(dbname string) error {
	dbname = strings.ToLower(dbname)

	h.currentdb = dbname

	db, err := h.store.GetDB(dbname + ".db")
	if err != nil {
		return err
	}

	h.db = db
	h.currentdb = dbname

	return nil
}

// HandleQuery - implements the `server.Handler` interface
func (h *SessionHandler) HandleQuery(query string) (*mysql.Result, error) {
	debug("Query: %s", query)
	parsed, err := sqlparser.Parse(query)
	if err != nil {
		return h.query(query)
	}

	switch parsed.(type) {
	default:
		return h.exec(query)
	case *sqlparser.Show:
		return h.query("SELECT name FROM sqlite_master WHERE type='table'")
	case *sqlparser.Set:
		return h.fakeOK(query)
	case *sqlparser.Select:
		return h.query(query)
	}
}

// HandleFieldList - implements the `server.Handler` interface
func (h *SessionHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, errors.New("<field.list> not supported yet")
}

// HandleStmtPrepare - implements the `server.Handler` interface
func (h *SessionHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	stmnt, err := sqlparser.Parse(query)
	if err != nil {
		return 0, 0, nil, err
	}

	res := sqlparser.GetBindvars(stmnt)

	_, err = h.db.Prepare(query)
	if err != nil {
		return 0, 0, nil, err
	}
	return len(res), len(res), nil, nil
}

// HandleStmtExecute - implements the `server.Handler` interface
func (h *SessionHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return h.exec(query, args...)
}

// HandleStmtClose - implements the `server.Handler` interface
func (h *SessionHandler) HandleStmtClose(context interface{}) error {
	return nil
}

// HandleOtherCommand - implements the `server.Handler` interface
func (h *SessionHandler) HandleOtherCommand(cmd byte, data []byte) error {
	debug("Other: %s %s", string(cmd), string(data))
	return errors.New("<other> not supported yet")
}

// fakeOK - returns a fake success result
func (h *SessionHandler) fakeOK(query string, args ...interface{}) (*mysql.Result, error) {
	ret := new(mysql.Result)
	ret.AffectedRows = 0

	return ret, nil
}

// exec - perform a query that writes data
func (h *SessionHandler) exec(query string, args ...interface{}) (*mysql.Result, error) {
	ret := new(mysql.Result)

	result, err := h.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	ret.AffectedRows = uint64(rows)
	ret.InsertId = uint64(lastID)

	return ret, nil
}

// query - perform a query that reads rows
func (h *SessionHandler) query(query string, args ...interface{}) (*mysql.Result, error) {
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := [][]interface{}{}

	for rows.Next() {
		args := make([]interface{}, len(fields))
		for i := range fields {
			var v interface{}
			args[i] = &v
		}
		if err := rows.Scan(args...); err != nil {
			return nil, err
		}
		for ii, val := range args {
			t := reflect.TypeOf((*(val.(*interface{}))))
			if nil == t {
				args[ii] = nil
				continue
			}

			switch t.Kind() {
			case reflect.Int64, reflect.Int32, reflect.Int:
				args[ii] = (*(val.(*interface{}))).(int64)
			case reflect.Float32, reflect.Float64:
				args[ii] = (*(val.(*interface{}))).(float64)
			case reflect.Slice:
				if t.Elem().Kind() == reflect.Uint || t.Elem().Kind() == reflect.Uint8 {
					args[ii] = string((*(val.(*interface{}))).([]uint8))
				} else {
					// TODO: handle that unknown type ?
				}
			default:
				args[ii] = (*(val.(*interface{}))).(string)
			}
		}
		values = append(values, args)
	}

	result, err := mysql.BuildSimpleResultset(fields, values, false)
	if err != nil {
		return nil, err
	}

	return &mysql.Result{
		Resultset: result,
	}, nil
}
