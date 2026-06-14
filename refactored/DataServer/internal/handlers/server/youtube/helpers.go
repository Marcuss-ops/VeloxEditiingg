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

	// Remove duplicates
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

