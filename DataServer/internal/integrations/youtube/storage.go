package youtube

import (
	"errors"
)

// Sentinel errors used across the youtube integration package.
//
// PR-YT-REPO: the Storage struct, StorageStore interface, variadic
// NewStorage and in-memory mode are deleted. These sentinels are
// retained because handlers/services export them as part of the
// public API contract.
var (
	ErrGroupExists         = errors.New("group already exists")
	ErrGroupNotFound       = errors.New("group not found")
	ErrTargetGroupNotFound = errors.New("target group not found")
	ErrChannelExists       = errors.New("channel already in group")
	ErrChannelNotFound     = errors.New("channel not found")
	ErrStoreNotConfigured  = errors.New("Repository not configured")
)
