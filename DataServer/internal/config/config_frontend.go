package config

import "os"

func loadFrontendConfig() FrontendConfig {
	c := FrontendConfig{
		SPADir:          os.Getenv("VELOX_SPA_DIR"),
		GradioAppURL:    os.Getenv("VELOX_GRADIO_APP_URL"),
		DarkEditorDir:   os.Getenv("VELOX_DARK_EDITOR_DIR"),
		DarkEditorProxy: os.Getenv("VELOX_DARK_EDITOR_PROXY_URL"),
	}
	if c.GradioAppURL == "" {
		c.GradioAppURL = "http://127.0.0.1:7860"
	}
	return c
}
