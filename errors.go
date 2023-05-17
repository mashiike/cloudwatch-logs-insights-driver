package cloudwatchlogsinsightsdriver

import "errors"

var (
	ErrNotSupported  = errors.New("not supported")
	ErrDSNEmpty      = errors.New("dsn is empty")
	ErrConnClosed    = errors.New("connection closed")
	ErrInvalidScheme = errors.New("invalid scheme")
)
