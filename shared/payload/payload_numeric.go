// Package payload / payload_numeric.go
//
// Numeric extraction + conversion helpers for map[string]interface{}
// (deserialized JSON). Split out of payload.go; the package doc lives
// in payload.go.
package payload

import (
	"encoding/json"
	"strconv"
	"strings"
)

func FloatParam(source map[string]interface{}, fallback float64, keys ...string) float64 {
	if source == nil {
		return fallback
	}
	for _, key := range keys {
		if v, ok := source[key]; ok {
			switch vv := v.(type) {
			case float64:
				if vv > 0 {
					return vv
				}
			case float32:
				if vv > 0 {
					return float64(vv)
				}
			case int:
				if vv > 0 {
					return float64(vv)
				}
			case int64:
				if vv > 0 {
					return float64(vv)
				}
			case json.Number:
				if f, err := vv.Float64(); err == nil && f > 0 {
					return f
				}
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(vv), 64); err == nil && f > 0 {
					return f
				}
			}
		}
	}
	return fallback
}

func IntParam(source map[string]interface{}, fallback int, keys ...string) int {
	if source == nil {
		return fallback
	}
	for _, key := range keys {
		if v, ok := source[key]; ok {
			switch vv := v.(type) {
			case int:
				if vv > 0 {
					return vv
				}
			case int64:
				if vv > 0 {
					return int(vv)
				}
			case float64:
				if vv > 0 {
					return int(vv)
				}
			case json.Number:
				if n, err := vv.Int64(); err == nil && n > 0 {
					return int(n)
				}
			case string:
				if n, err := strconv.Atoi(strings.TrimSpace(vv)); err == nil && n > 0 {
					return n
				}
			}
		}
	}
	return fallback
}

func EnsureInt(value interface{}, fallback int) int {
	switch v := value.(type) {
	case int:
		if v > 0 {
			return v
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	case float64:
		if v > 0 {
			return int(v)
		}
	case json.Number:
		if n, err := v.Int64(); err == nil && n > 0 {
			return int(n)
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func NormalizedDuration(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return f
	default:
		return 0
	}
}

// ParseInt converte una string in int, ignorando errori.
func ParseInt(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

// ParseIntDef converte una string in int, con fallback se vuota, non valida o <= 0.
func ParseIntDef(s string, def int) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// ParseIntParam converte una string in int, restituendo l'errore di parsing.
// Restituisce il default se la string è vuota.
func ParseIntParam(s string, def int) (int, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return def, err
	}
	return n, nil
}

// ParseInt64Param converte una string in int64, restituendo l'errore di parsing.
// Restituisce il default se la string è vuota.
func ParseInt64Param(s string, def int64) (int64, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return def, err
	}
	return n, nil
}

// ParseFloatParam converte una string in float64, restituendo l'errore di parsing.
// Restituisce il default se la string è vuota.
func ParseFloatParam(s string, def float64) (float64, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return def, err
	}
	return f, nil
}

// AsInt converte un interface{} in int, gestendo int, int64, float64, json.Number e string.
func AsInt(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return int(n)
		}
	default:
		if s, ok := v.(string); ok {
			if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
				return n
			}
		}
	}
	return 0
}

// FloatValue estrae un valore float64 da una mappa SENZA il guard > 0.
// A differenza di FloatParam, restituisce il valore raw anche se 0 o negativo.
// Utile per dati analytics dove 0 è un valore legittimo.
func FloatValue(data map[string]interface{}, key string) float64 {
	if data == nil {
		return 0
	}
	if v, ok := data[key]; ok {
		switch n := v.(type) {
		case float64:
			return n
		case float32:
			return float64(n)
		case int:
			return float64(n)
		case int64:
			return float64(n)
		case json.Number:
			if f, err := n.Float64(); err == nil {
				return f
			}
		}
	}
	return 0
}

// AsFloat converte un interface{} in float64, gestendo float64, float32, int, int64 e json.Number.
func AsFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		n, _ := t.Float64()
		return n
	default:
		return 0
	}
}
