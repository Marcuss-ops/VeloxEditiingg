package dbutil

import "testing"

func TestIntFromMap_TypedInts(t *testing.T) {
	cases := []struct {
		name string
		m    map[string]interface{}
		key  string
		want int
	}{
		{"int", map[string]interface{}{"k": 7}, "k", 7},
		{"int8", map[string]interface{}{"k": int8(7)}, "k", 7},
		{"int16", map[string]interface{}{"k": int16(7)}, "k", 7},
		{"int32", map[string]interface{}{"k": int32(7)}, "k", 7},
		{"int64", map[string]interface{}{"k": int64(7)}, "k", 7},
		{"uint", map[string]interface{}{"k": uint(7)}, "k", 7},
		{"uint64", map[string]interface{}{"k": uint64(7)}, "k", 7},
		{"float64", map[string]interface{}{"k": 7.0}, "k", 7},
		{"float32", map[string]interface{}{"k": float32(7)}, "k", 7},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IntFromMap(c.m, c.key); got != c.want {
				t.Errorf("got %d, want %d", got, c.want)
			}
		})
	}
}

func TestIntFromMap_StringJSON(t *testing.T) {
	// JSON-decoded payloads serialise numbers as string in some legacy paths.
	if got := IntFromMap(map[string]interface{}{"k": "12"}, "k"); got != 12 {
		t.Errorf("string \"12\": got %d, want 12", got)
	}
	if got := IntFromMap(map[string]interface{}{"k": ""}, "k"); got != 0 {
		t.Errorf("empty string: got %d, want 0", got)
	}
	if got := IntFromMap(map[string]interface{}{"k": "abc"}, "k"); got != 0 {
		t.Errorf("malformed string: got %d, want 0", got)
	}
}

func TestIntFromMap_MissingOrNil(t *testing.T) {
	if got := IntFromMap(map[string]interface{}{}, "ghost"); got != 0 {
		t.Errorf("missing key: got %d, want 0", got)
	}
	if got := IntFromMap(map[string]interface{}{"k": nil}, "k"); got != 0 {
		t.Errorf("nil value: got %d, want 0", got)
	}
	// nil receiver must not panic.
	if got := IntFromMap(nil, "k"); got != 0 {
		t.Errorf("nil map: got %d, want 0", got)
	}
}

func TestIntFromMap_UnsupportedType(t *testing.T) {
	// Unsupported types yield 0 rather than panicking.
	m := map[string]interface{}{"k": struct{}{}}
	if got := IntFromMap(m, "k"); got != 0 {
		t.Errorf("struct: got %d, want 0", got)
	}
}

func TestIntFromMap_NegativeAndLarge(t *testing.T) {
	// Real SQLite values often come through as float64 with negative / 64-bit range.
	if got := IntFromMap(map[string]interface{}{"k": -1.0}, "k"); got != -1 {
		t.Errorf("neg float: got %d, want -1", got)
	}
	if got := IntFromMap(map[string]interface{}{"k": int64(1 << 40)}, "k"); got != 1<<40 {
		t.Errorf("large int64 (1<<40): got %d, want %d", got, 1<<40)
	}
}
