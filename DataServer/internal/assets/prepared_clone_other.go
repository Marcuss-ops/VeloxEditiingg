//go:build !linux

package assets

import "errors"

var errPreparedCloneUnsupported = errors.New("prepared fragment clone is unsupported on this platform")

func clonePreparedFile(string, string) error { return errPreparedCloneUnsupported }
