package youtube

import (
	"fmt"
	"path/filepath"
	"strings"
)

func parseTags(tagsStr string) []string {
	if tagsStr == "" {
		return []string{}
	}
	return strings.Split(tagsStr, ",")
}

func generateMockTitles(fileName, customPrompt string) []string {
	baseName := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	baseName = strings.ReplaceAll(baseName, "_", " ")
	baseName = strings.ReplaceAll(baseName, "-", " ")

	return []string{
		fmt.Sprintf("%s - Complete Guide 2025", strings.Title(baseName)),
		fmt.Sprintf("How to Master %s in 10 Minutes", strings.Title(baseName)),
		fmt.Sprintf("%s Explained: Everything You Need to Know", strings.Title(baseName)),
		fmt.Sprintf("The Ultimate %s Tutorial", strings.Title(baseName)),
		fmt.Sprintf("%s Tips and Tricks You Need to See", strings.Title(baseName)),
	}
}

func generateMockDescription(title, customPrompt string) string {
	return fmt.Sprintf(`%s

In this video, we dive deep into the topic and explore all the key aspects you need to know.

📌 Timestamps:
0:00 - Introduction
2:30 - Main Content
8:45 - Key Takeaways
12:00 - Conclusion

🔔 Subscribe for more content!
💬 Leave a comment if you have any questions!

#youtube #tutorial #2025 #guide`, title)
}

func generateMockTags(title, customPrompt string) []string {
	words := strings.Fields(strings.ToLower(title))
	tags := []string{"tutorial", "how to", "guide", "2025", "tips", "tricks"}

	for _, w := range words {
		if len(w) > 3 {
			tags = append(tags, w)
		}
	}

	seen := make(map[string]bool)
	result := []string{}
	for _, t := range tags {
		if !seen[t] {
			seen[t] = true
			result = append(result, t)
		}
	}

	return result[:min(10, len(result))]
}

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
