package filesql

import "log/slog"

// newNopLogger returns the logger a builder starts with, which discards
// everything written to it.
//
// A discarding logger rather than a nil one is what lets every logging call in
// this package be an unguarded method call: nothing has to ask whether a logger
// was configured before saying something about what it is doing.
func newNopLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
