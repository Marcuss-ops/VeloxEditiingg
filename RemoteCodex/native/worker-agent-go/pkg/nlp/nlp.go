// Package nlp provides lightweight NLP utilities for VeloxEditing
//
// This package uses jdkato/prose for NLP tasks without heavy dependencies.
// No torch, spacy, or other heavy runtimes are required.
//
// Features:
//   - Sentence splitting
//   - Word tokenization
//   - Part-of-speech tagging
//   - Named entity recognition
//
// Performance:
//   - Lightweight (no heavy ML models)
//   - Fast processing
//   - Low memory footprint
package nlp

import (
	"strings"

	"github.com/jdkato/prose/v2"
)

// SplitSentences splits text into sentences using prose
//
// This function uses the prose library for accurate sentence boundary
// detection. It handles:
//   - Abbreviations (e.g., "Mr.", "Dr.", "U.S.A.")
//   - Numbers with periods (e.g., "3.14")
//   - Multiple languages
//
// Example:
//
//	text := "Hello world. How are you? I'm fine!"
//	sentences := SplitSentences(text)
//	// sentences = ["Hello world.", "How are you?", "I'm fine!"]
func SplitSentences(text string) []string {
	if text == "" {
		return []string{}
	}

	doc, err := prose.NewDocument(text)
	if err != nil {
		// Fallback to simple splitting if prose fails
		return simpleSplitSentences(text)
	}

	sentences := make([]string, 0, len(doc.Sentences()))
	for _, sent := range doc.Sentences() {
		s := strings.TrimSpace(sent.Text)
		if s != "" {
			sentences = append(sentences, s)
		}
	}

	return sentences
}

// simpleSplitSentences is a fallback sentence splitter
func simpleSplitSentences(text string) []string {
	if text == "" {
		return []string{}
	}

	// Simple splitting on sentence-ending punctuation
	var sentences []string
	var current strings.Builder

	for _, r := range text {
		current.WriteRune(r)
		if r == '.' || r == '!' || r == '?' {
			s := strings.TrimSpace(current.String())
			if s != "" {
				sentences = append(sentences, s)
			}
			current.Reset()
		}
	}

	// Add remaining text
	if current.Len() > 0 {
		s := strings.TrimSpace(current.String())
		if s != "" {
			sentences = append(sentences, s)
		}
	}

	return sentences
}

// TokenizeWords tokenizes text into words using prose
//
// This function uses the prose library for accurate word tokenization.
// It handles:
//   - Unicode word boundaries
//   - Contractions (e.g., "don't" → ["don't"])
//   - Punctuation
//
// Example:
//
//	text := "Hello world! How are you?"
//	words := TokenizeWords(text)
//	// words = ["Hello", "world", "!", "How", "are", "you", "?"]
func TokenizeWords(text string) []string {
	if text == "" {
		return []string{}
	}

	doc, err := prose.NewDocument(text)
	if err != nil {
		// Fallback to simple tokenization if prose fails
		return simpleTokenizeWords(text)
	}

	tokens := doc.Tokens()
	words := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		if tok.Text != "" {
			words = append(words, tok.Text)
		}
	}

	return words
}

// simpleTokenizeWords is a fallback word tokenizer
func simpleTokenizeWords(text string) []string {
	if text == "" {
		return []string{}
	}

	// Simple whitespace-based tokenization
	return strings.Fields(text)
}

// ExtractEntities extracts named entities from text using prose
//
// This function uses the prose library for named entity recognition.
// It identifies:
//   - Person names
//   - Organization names
//   - Location names
//   - Dates
//   - Monetary values
//
// Example:
//
//	text := "Apple Inc. was founded by Steve Jobs in Cupertino."
//	entities := ExtractEntities(text)
//	// entities = [
//	//   {Text: "Apple Inc.", Label: "ORG"},
//	//   {Text: "Steve Jobs", Label: "PERSON"},
//	//   {Text: "Cupertino", Label: "GPE"},
//	// ]
type Entity struct {
	Text  string
	Label string
}

func ExtractEntities(text string) []Entity {
	if text == "" {
		return []Entity{}
	}

	doc, err := prose.NewDocument(text)
	if err != nil {
		return []Entity{}
	}

	entities := make([]Entity, 0)
	for _, ent := range doc.Entities() {
		entities = append(entities, Entity{
			Text:  ent.Text,
			Label: ent.Label,
		})
	}

	return entities
}

// CountWords counts the number of words in text (Unicode-aware)
func CountWords(text string) int {
	if text == "" {
		return 0
	}

	return len(TokenizeWords(text))
}

// ExtractUniqueWords extracts unique words from text (case-insensitive)
func ExtractUniqueWords(text string) []string {
	if text == "" {
		return []string{}
	}

	words := TokenizeWords(text)
	seen := make(map[string]bool)
	unique := make([]string, 0)

	for _, word := range words {
		lower := strings.ToLower(word)
		if !seen[lower] {
			seen[lower] = true
			unique = append(unique, lower)
		}
	}

	return unique
}
