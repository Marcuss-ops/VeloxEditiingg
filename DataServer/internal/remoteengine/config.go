package remoteengine

import (
	"os"
	"strconv"
)

// DefaultConfig returns config from environment.
func DefaultConfig() Config {
	timeoutMS := 60000 // default 60s
	if v := os.Getenv("VELOX_REMOTE_ENGINE_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			timeoutMS = n
		}
	}
	retries := 3
	if v := os.Getenv("VELOX_REMOTE_ENGINE_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			retries = n
		}
	}
	return Config{
		URL:       os.Getenv("VELOX_REMOTE_ENGINE_URL"),
		Token:     os.Getenv("VELOX_REMOTE_ENGINE_TOKEN"),
		TimeoutMS: timeoutMS,
		Retries:   retries,
	}
}
