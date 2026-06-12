package youtube

import (
	"context"
	"regexp"
	"strings"
)

// DetectChannelLanguage attempts to detect the language of a YouTube channel.
func (s *Service) DetectChannelLanguage(ctx context.Context, channelID string, channelName string) string {
	if ch := s.GetAuthChannel(channelID); ch != nil && ch.Language != "" {
		return ch.Language
	}

	tryAPIDetect := func() string {
		ytService, err := s.GetYouTubeService(ctx, channelID)
		if err != nil {
			return ""
		}
		resp, err := ytService.Channels.List([]string{"snippet"}).Id(channelID).Do()
		if err != nil || len(resp.Items) == 0 {
			return ""
		}
		lang := resp.Items[0].Snippet.DefaultLanguage
		if lang != "" && isValidLanguageCode(lang) {
			return lang
		}
		country := resp.Items[0].Snippet.Country
		if country != "" {
			if code := countryToLanguage(country); code != "" {
				return code
			}
		}
		return ""
	}

	if lang := tryAPIDetect(); lang != "" {
		return lang
	}

	return DetectLanguageFromName(channelName)
}

func isValidLanguageCode(code string) bool {
	knownCodes := map[string]bool{
		"en": true, "es": true, "fr": true, "de": true, "it": true,
		"pt": true, "ru": true, "ja": true, "ko": true, "zh": true,
		"ar": true, "hi": true, "nl": true, "pl": true, "tr": true,
		"sv": true, "da": true, "fi": true, "no": true, "cs": true,
		"hu": true, "ro": true, "th": true, "vi": true, "el": true,
		"he": true, "id": true, "ms": true, "tl": true, "uk": true,
	}
	return knownCodes[code]
}

func countryToLanguage(country string) string {
	mapping := map[string]string{
		"US": "en", "GB": "en", "AU": "en", "CA": "en",
		"IT": "it", "FR": "fr", "DE": "de", "ES": "es",
		"PT": "pt", "BR": "pt", "RU": "ru", "JP": "ja",
		"KR": "ko", "CN": "zh", "TW": "zh", "SA": "ar",
		"IN": "hi", "NL": "nl", "PL": "pl", "TR": "tr",
		"SE": "sv", "DK": "da", "FI": "fi", "NO": "no",
		"TH": "th", "VN": "vi", "GR": "el", "IL": "he",
		"ID": "id", "UA": "uk",
	}
	return mapping[country]
}

func hasWord(text, word string) bool {
	if len(word) > 2 {
		return strings.Contains(text, word)
	}
	pattern := `(?i)(^|[\s_\-\.\/])` + regexp.QuoteMeta(word) + `($|[\s_\-\.\/])`
	matched, _ := regexp.MatchString(pattern, text)
	return matched
}

// DetectLanguageFromName attempts to detect language from channel name/title using Unicode ranges and keywords
func DetectLanguageFromName(name string) string {
	if name == "" {
		return "en"
	}

	if strings.HasPrefix(name, "UC") && len(name) == 24 {
		return "unknown"
	}

	hasCyrillic := false
	hasJapanese := false
	hasChinese := false
	hasKorean := false
	hasArabic := false
	hasHindi := false

	for _, r := range name {
		switch {
		case r >= 0x0400 && r <= 0x04FF:
			hasCyrillic = true
		case r >= 0x3040 && r <= 0x309F, r >= 0x30A0 && r <= 0x30FF:
			hasJapanese = true
		case r >= 0x4E00 && r <= 0x9FFF:
			hasChinese = true
		case r >= 0xAC00 && r <= 0xD7AF:
			hasKorean = true
		case r >= 0x0600 && r <= 0x06FF:
			hasArabic = true
		case r >= 0x0900 && r <= 0x097F:
			hasHindi = true
		}
	}

	if hasJapanese {
		return "ja"
	}
	if hasKorean {
		return "ko"
	}
	if hasChinese {
		return "zh"
	}
	if hasCyrillic {
		return "ru"
	}
	if hasArabic {
		return "ar"
	}
	if hasHindi {
		return "hi"
	}

	lower := strings.ToLower(name)

	italianKeywords := []string{"it", "italia", "italiano", "pizza", "mamma", "ciao", "buongiorno", "canale", "video", "ufficiale"}
	for _, kw := range italianKeywords {
		if hasWord(lower, kw) {
			return "it"
		}
	}

	frenchKeywords := []string{"fr", "france", "français", "francaise", "bonjour", "chaîne", "officiel", "paris"}
	for _, kw := range frenchKeywords {
		if hasWord(lower, kw) {
			return "fr"
		}
	}

	germanKeywords := []string{"de", "deutsch", "german", "kanal", "offiziell", "berlin"}
	for _, kw := range germanKeywords {
		if hasWord(lower, kw) {
			return "de"
		}
	}

	spanishKeywords := []string{"es", "españa", "espana", "espanol", "español", "canal", "oficial", "madrid"}
	for _, kw := range spanishKeywords {
		if hasWord(lower, kw) {
			return "es"
		}
	}

	portugueseKeywords := []string{"pt", "portugal", "português", "portugues", "brasil", "canal"}
	for _, kw := range portugueseKeywords {
		if hasWord(lower, kw) {
			return "pt"
		}
	}

	polishKeywords := []string{"pl", "polska", "polski", "kanał", "oficjalny"}
	for _, kw := range polishKeywords {
		if hasWord(lower, kw) {
			return "pl"
		}
	}

	turkishKeywords := []string{"tr", "türk", "turk", "türkiye", "kanal", "resmi"}
	for _, kw := range turkishKeywords {
		if hasWord(lower, kw) {
			return "tr"
		}
	}

	return "en"
}
