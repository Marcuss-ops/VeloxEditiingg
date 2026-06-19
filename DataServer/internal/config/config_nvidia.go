package config

import "os"

func loadNVIDIAConfig() NVIDIAConfig {
	return NVIDIAConfig{
		APIKey:  os.Getenv("VELOX_NVIDIA_API_KEY"),
		TextURL: os.Getenv("VELOX_NVIDIA_TEXT_URL"),
	}
}
