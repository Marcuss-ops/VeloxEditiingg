package workercache

import "os"

// cacheFileSystem is the narrow filesystem boundary used by cache eviction.
// Keeping it internal preserves the public Cache API while making physical
// removal independently testable and preventing policy code from importing os.
type cacheFileSystem interface {
	Remove(string) error
}

type osCacheFileSystem struct{}

func (osCacheFileSystem) Remove(path string) error { return os.Remove(path) }
