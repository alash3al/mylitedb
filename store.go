package main

import (
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
)

// Store - databases container
type Store struct {
	basedir   string
	databases map[string]*sql.DB
	sync.RWMutex
}

// NewStore - initialize a new store container
func NewStore(dir string) (*Store, error) {
	s := new(Store)
	s.basedir = dir
	s.databases = map[string]*sql.DB{}

	s.Lock()
	defer s.Unlock()

	files, err := filepath.Glob(filepath.Join(s.basedir, "*.db"))
	if err != nil {
		return nil, err
	}

	for _, filename := range files {
		db, err := s.openlite(filename)
		if err != nil {
			return nil, err
		}

		if err := db.Ping(); err != nil {
			return nil, err
		}

		s.databases[strings.ToLower(strings.Split(filepath.Base(filename), ".db")[0])] = db
	}

	return s, nil
}

// GetDB - fetch a db from the store
func (s *Store) GetDB(name string) (*sql.DB, error) {
	name = strings.ToLower(name)

	s.Lock()
	defer s.Unlock()

	if db := s.databases[name]; db != nil {
		return db, nil
	}

	db, err := s.openlite(filepath.Join(s.basedir, name))
	if err != nil {
		return nil, err
	}

	s.databases[name] = db

	return db, nil
}

// openlite - opens a sqlite database
func (s *Store) openlite(filename string) (*sql.DB, error) {
	return sql.Open("sqlite3", filename+"?"+*flagOptions)
}
