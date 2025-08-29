package storage

import "errors"

var (
	ErrFieldNotFound       = errors.New("field not found")
	ErrTransactionConflict = errors.New("transaction conflict: key was modified by another client")
)
