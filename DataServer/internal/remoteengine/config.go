package remoteengine

import "velox-server/internal/config"

// ConfigFromRuntime adapts the centrally parsed render configuration.
func ConfigFromRuntime(c config.RenderConfig) Config {
	return Config{
		URL:       c.RemoteEngineURL,
		Token:     c.RemoteEngineToken,
		TimeoutMS: c.RemoteEngineTimeoutMS,
		Retries:   c.RemoteEngineRetries,
	}
}
