// Package metrics / fmt.go
//
// Tiny shim around strconv.FormatFloat that lives in its own file to
// keep the dependency surface of metrics.go minimal and readable.
package metrics

import "strconv"

// strconvFormatFloat mirrors strconv.FormatFloat here so callers in
// metrics.go don't have to import strconv directly. Kept in a tiny
// standalone file so package-scoped variable definitions can lean on
// it without creating an import cycle.
func strconvFormatFloat(v float64, fmt byte, prec, bitSize int) string {
	return strconv.FormatFloat(v, fmt, prec, bitSize)
}
