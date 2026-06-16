package youtube

import (
	"fmt"
	"strings"
)

func extractVideoID(url string) string {
	if strings.Contains(url, "youtu.be/") {
		parts := strings.Split(url, "youtu.be/")
		if len(parts) > 1 {
			return strings.Split(strings.Split(parts[1], "?")[0], "/")[0]
		}
	}
	if strings.Contains(url, "watch?v=") {
		parts := strings.Split(url, "watch?v=")
		if len(parts) > 1 {
			return strings.Split(strings.Split(parts[1], "&")[0], "#")[0]
		}
	}
	if strings.Contains(url, "/shorts/") {
		parts := strings.Split(url, "/shorts/")
		if len(parts) > 1 {
			return strings.Split(strings.Split(parts[1], "?")[0], "/")[0]
		}
	}
	if strings.Contains(url, "/embed/") {
		parts := strings.Split(url, "/embed/")
		if len(parts) > 1 {
			return strings.Split(strings.Split(parts[1], "?")[0], "/")[0]
		}
	}
	return ""
}

func parseIntParam(s string, def int) (int, error) {
	if s == "" {
		return def, nil
	}
	var i int
	_, err := fmt.Sscanf(s, "%d", &i)
	if err != nil {
		return def, err
	}
	return i, nil
}

func parseIntParam64(s string, def int64) (int64, error) {
	if s == "" {
		return def, nil
	}
	var i int64
	_, err := fmt.Sscanf(s, "%d", &i)
	if err != nil {
		return def, err
	}
	return i, nil
}

func parseFloatParam(s string, def float64) (float64, error) {
	if s == "" {
		return def, nil
	}
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	if err != nil {
		return def, err
	}
	return f, nil
}
