package skiff

import "errors"

var (
	// ErrNotFound means the key is absent (distinct from an empty stored value).
	ErrNotFound = errors.New("skiff: key not found")
	// ErrClosed means the database handle is closed.
	ErrClosed = errors.New("skiff: database closed")
	// ErrInvalidRecord means a WAL record failed integrity checks.
	ErrInvalidRecord = errors.New("skiff: invalid record")
)
