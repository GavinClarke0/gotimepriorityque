package gotimepriorityque

import (
	"errors"
)

var (
	// ErrIncompatibleType is returned when the opener type is
	// incompatible with the stored Goque type.
	ErrIncompatibleType = errors.New("gotimepriorityque: Opener type is incompatible with stored Goque type")

	// ErrEmpty is returned when the stack or queue is empty.
	ErrEmpty = errors.New("gotimepriorityque: Stack or queue is empty")

	// ErrOutOfBounds is returned when the ID used to lookup an item
	// is outside of the range of the stack or queue.
	ErrOutOfBounds = errors.New("gotimepriorityque: ID used is outside range of stack or queue")

	// ErrDBClosed is returned when the Close function has already
	// been called, causing the stack or queue to close, as well as
	// its underlying database.
	ErrDBClosed = errors.New("gotimepriorityque: Database is closed")
)
