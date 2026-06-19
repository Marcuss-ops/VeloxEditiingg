// Package news provides RSS parsing for Google News
package news

import (
	"encoding/xml"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// rssFeed represents the Google News RSS structure
type rssFeed struct {
	XMLName xml.Name `xml:"rss"`
	Channel struct {
		Title string `xml:"title"`
		Items []struct {
			Title       string `xml:"title"`
			Link        string `xml:"link"`
			Description string `xml:"description"`
			PubDate     string `xml:"pubDate"`
			Source      string `xml:"source"`
		} `xml:"item"`
	} `xml:"channel"`
}

// parseGoogleNewsRSS parses Google News RSS XML into NewsItem slice
func parseGoogleNewsRSS(rssContent string, limit int) ([]NewsItem, error) {
	var feed rssFeed
	if err := xml.Unmarshal([]byte(rssContent), &feed); err != nil {
		return nil, fmt.Errorf("rss parse error: %w", err)
	}

	var news []NewsItem
	for _, item := range feed.Channel.Items {
		if len(news) >= limit {
			break
		}

		// Parse pub date (Mon, 02 Jan 2006 15:04:05 GMT)
		pubTime, _ := time.Parse(time.RFC1123Z, item.PubDate)
		if pubTime.IsZero() {
			pubTime = time.Now()
		}

		// Clean description (remove HTML tags)
		description := stripHTML(item.Description)
		if len(description) > 300 {
			description = description[:300] + "..."
		}

		// Extract source from item
		source := item.Source
		if source == "" {
			source = "Google News"
		}

		news = append(news, NewsItem{
			Title:       item.Title,
			URL:         item.Link,
			Source:      source,
			PublishedAt: pubTime,
			Description: description,
		})
	}

	return news, nil
}

// stripHTML removes HTML tags from a string
func stripHTML(s string) string {
	re := regexp.MustCompile(`<[^>]*>`)
	result := re.ReplaceAllString(s, "")
	// Decode common HTML entities
	result = strings.ReplaceAll(result, "&amp;", "&")
	result = strings.ReplaceAll(result, "&lt;", "<")
	result = strings.ReplaceAll(result, "&gt;", ">")
	result = strings.ReplaceAll(result, "&#39;", "'")
	result = strings.ReplaceAll(result, "&quot;", "\"")
	return strings.TrimSpace(result)
}
