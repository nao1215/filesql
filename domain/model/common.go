// Package model provides domain model for filesql
package model

import "errors"

// ErrDuplicateColumnName is returned when a file contains duplicate column names
var ErrDuplicateColumnName = errors.New("duplicate column name")

// Header is file header.
type Header []string

// NewHeader create new Header.
func NewHeader(h []string) Header {
	return Header(h)
}

// Equal compare Header.
func (h Header) Equal(h2 Header) bool {
	if len(h) != len(h2) {
		return false
	}
	for i, v := range h {
		if v != h2[i] {
			return false
		}
	}
	return true
}

// Record is file records.
type Record []string

// NewRecord create new Record.
func NewRecord(r []string) Record {
	return Record(r)
}

// Equal compare Record.
func (r Record) Equal(r2 Record) bool {
	if len(r) != len(r2) {
		return false
	}
	for i, v := range r {
		if v != r2[i] {
			return false
		}
	}
	return true
}
