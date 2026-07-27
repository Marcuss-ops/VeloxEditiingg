package pipeline

import (
	"strings"
	"testing"
)

// ── IdempotencyKey validation ────────────────────────────────────
//
// ValidateIdempotencyKey's contract is enforced entirely by Table-driven
// cases here so that future drift (e.g., accidentally allowing ":" or
// silently truncating) is caught at unit-test time without needing a
// running Velox server.
//
// Each sub-case captures: input, expected rejection reason, and any
// expected diagnostics (length, byte_offset). The contract-level
// invariants under test:
//
//   (1) Empty / whitespace-only input is rejected with reason="empty".
//   (2) Length > MaxIdempotencyKeyLen is rejected with reason="length"
//       and the observed length reported back to the client. NEVER
//       truncated silently.
//   (3) Invalid UTF-8 (e.g., a stray continuation byte) is rejected
//       with reason="encoding" and the OFFSET of the bad byte. NEVER
//       silently replaced with U+FFFD.
//   (4) Forbidden bytes (':', '%', ASCII control chars, NUL, space,
//       DEL) are each rejected with reason="control_char" and their
//       byte offset.
//   (5) Otherwise: accepted (nil error).
//   (6) Post-trim canonicalization: leading/trailing whitespace is
//       stripped without error; an all-whitespace string becomes empty
//       and triggers reason="empty".

// TestValidateIdempotencyKey is the table-driven umbrella for §1-§6.
// Each row is documented inline so a regression here is immediately
// actionable without spelunking through the helper.
func TestValidateIdempotencyKey(t *testing.T) {
	t.Parallel()

	// Few helpers to make the cases readable.
	// badUTF8 starts with a valid ASCII 'a' then a stray continuation
	// byte 0x80 (no preceding lead byte). The first invalid byte
	// offset must be 1 (the position of 0x80).
	badUTF8 := string([]byte{'a', 0x80, 'b'})

	cases := []struct {
		name         string
		input        string
		wantReject   bool
		wantReason   string
		wantLength   *int
		wantByteOff  *int
	}{
		// ── §1 — empty / whitespace ─────────────────────────────────
		{name: "empty_string",      input: "",             wantReject: true, wantReason: "empty"},
		{name: "all_spaces",        input: "   ",          wantReject: true, wantReason: "empty"},
		{name: "tab_newline_only",  input: "\t\n",         wantReject: true, wantReason: "empty"},
		// ── §2 — length ─────────────────────────────────────────────
		{
			name:        "one_below_max",
			input:       strings.Repeat("a", MaxIdempotencyKeyLen-1),
			wantReject:  false,
		},
		{
			name:        "exactly_max",
			input:       strings.Repeat("a", MaxIdempotencyKeyLen),
			wantReject:  false,
		},
		{
			name:       "one_above_max",
			input:      strings.Repeat("a", MaxIdempotencyKeyLen+1),
			wantReject: true,
			wantReason: "length",
			wantLength: intPtr(MaxIdempotencyKeyLen + 1),
		},
		// ── §3 — encoding ───────────────────────────────────────────
		{
			name:        "invalid_utf8",
			input:       badUTF8,
			wantReject:  true,
			wantReason:  "encoding",
			wantLength:  intPtr(3),
			wantByteOff: intPtr(1),
		},
		// ── §4 — forbidden byte classes ─────────────────────────────
		{
			name:        "colon_rejected",
			input:       "abc:def",
			wantReject:  true,
			wantReason:  "control_char",
			wantByteOff: intPtr(3),
		},
		{
			name:        "percent_rejected",
			input:       "abc%def",
			wantReject:  true,
			wantReason:  "control_char",
			wantByteOff: intPtr(3),
		},
		{
			name:        "nul_rejected",
			input:       "abc\x00def",
			wantReject:  true,
			wantReason:  "control_char",
			wantByteOff: intPtr(3),
		},
		{
			name:        "lf_rejected",
			input:       "abc\ndef",
			wantReject:  true,
			wantReason:  "control_char",
			wantByteOff: intPtr(3),
		},
		{
			name:        "tab_rejected",
			input:       "abc\tdef",
			wantReject:  true,
			wantReason:  "control_char",
			wantByteOff: intPtr(3),
		},
		{
			name:        "space_rejected",
			input:       "abc def",
			wantReject:  true,
			wantReason:  "control_char",
			wantByteOff: intPtr(3),
		},
		{
			name:        "del_rejected",
			input:       "abc\x7Fdef",
			wantReject:  true,
			wantReason:  "control_char",
			wantByteOff: intPtr(3),
		},
		// ── §5 — happy path ─────────────────────────────────────────
		{name: "lowercase_alpha",   input: "abc-123",      wantReject: false},
		{name: "alphanumeric_mixed", input: "Video_001",   wantReject: false},
		{name: "utf8_happy",        input: "vidéo-01",     wantReject: false}, // multi-byte OK
		{name: "punctuation_ok",    input: "vid.01+a@b-c", wantReject: false},
		// ── §6 — post-trim canonicalization ─────────────────────────
		{name: "trims_leading",     input: "   abc",       wantReject: false},
		{name: "trims_trailing",    input: "abc\t\n",      wantReject: false}, // TrimSpace removes \t \n
		{name: "trims_both",        input: "\n  abc  \r\n", wantReject: false},
		// 4-byte UTF-8 emoji straddling max boundary would split
		// under byte-truncation; here we just verify that's accepted
		// when the total rune count fits (and the byte-length fits).
		{name: "emoji_3_bytes",     input: "🚀abc",        wantReject: false},
		// The "passes_max_with_emoji" check is implicit: a 128-byte
		// string of 1-byte runes is acceptable, and a 128-byte
		// string with 4-byte runes ALSO is acceptable (because the
		// cap is BYTE-length, not RUNE-length, and byte-correctness
		// is what the forwarding key needs anyway).
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err, bad := ValidateIdempotencyKey(tc.input)
			if bad != tc.wantReject {
				t.Fatalf("ValidateIdempotencyKey(%q) rejected=%v want=%v (err=%v)",
					tc.input, bad, tc.wantReject, err)
			}
			if !bad {
				if err != nil {
					t.Fatalf("ValidateIdempotencyKey(%q) accepted but returned err=%v",
						tc.input, err)
				}
				return
			}
			// bad=true => err != nil is a contract.
			if err == nil {
				t.Fatalf("ValidateIdempotencyKey(%q) rejected but returned nil err", tc.input)
			}
			if err.Reason != tc.wantReason {
				t.Errorf("ValidateIdempotencyKey(%q) reason=%q want=%q (code=%s, message=%q)",
					tc.input, err.Reason, tc.wantReason, err.Code, err.Message)
			}
			if tc.wantLength != nil {
				if err.FieldLength == nil {
					t.Errorf("ValidateIdempotencyKey(%q) FieldLength=nil want=%d",
						tc.input, *tc.wantLength)
				} else if *err.FieldLength != *tc.wantLength {
					t.Errorf("ValidateIdempotencyKey(%q) FieldLength=%d want=%d",
						tc.input, *err.FieldLength, *tc.wantLength)
				}
			}
			if tc.wantByteOff != nil {
				if err.FieldByteOff == nil {
					t.Errorf("ValidateIdempotencyKey(%q) FieldByteOff=nil want=%d",
						tc.input, *tc.wantByteOff)
				} else if *err.FieldByteOff != *tc.wantByteOff {
					t.Errorf("ValidateIdempotencyKey(%q) FieldByteOff=%d want=%d",
						tc.input, *err.FieldByteOff, *tc.wantByteOff)
				}
			}
		})
	}
}

// TestValidateIdempotencyKey_AllRejectionsReturnTypedCode locks the
// error-shape invariant: every rejection emits a structured error with
// Code="invalid_payload". The HTTP layer is responsible for the status
// code; the validator emits the machine-readable identifier that the
// API client uses to discriminate between "length" vs "encoding" vs
// "control_char".
//
// Splitting this from the table-driven test means a future regression
// that defaults to a more generic "bad_request" code (which the API
// would treat as a generic 400) still gets caught.
func TestValidateIdempotencyKey_AllRejectionsReturnTypedCode(t *testing.T) {
	t.Parallel()

	rejectInputs := []string{
		"",
		strings.Repeat("a", MaxIdempotencyKeyLen+1),
		"a\x80b",  // invalid utf8
		"a:b",     // colon
		"a%b",     // percent
		"a\x00b",  // NUL
		"a b",     // space
	}

	for _, in := range rejectInputs {
		err, bad := ValidateIdempotencyKey(in)
		if !bad {
			t.Errorf("input %q unexpectedly accepted", in)
			continue
		}
		if err.Code != "invalid_payload" {
			t.Errorf("input %q: Code=%q, want invalid_payload", in, err.Code)
		}
		// Message MUST be non-empty so the API envelope is actionable.
		if err.Message == "" {
			t.Errorf("input %q: empty Message", in)
		}
	}
}

// TestFirstInvalidUTF8Offset spot-checks the helper directly because
// it underpins the encoding-error byte_offset diagnostic. If the
// helper regresses to "always return -1", the encoding rejection
// stops being useful, but the table-driven test would still pass
// (the test only asserts the offset IS what we expect when present).
func TestFirstInvalidUTF8Offset(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want int
	}{
		{"ascii_clean", "abc", -1},
		{"utf8_clean",  "héllo", -1},                       // é = 2 bytes
		{"utf8_clean_4b", "🚀abc", -1},                     // 🚀 = 4 bytes
		{"invalid_at_0", string([]byte{0x80}) + "abc", 0},  // lone continuation
		{"invalid_at_1", "a" + string([]byte{0x80}), 1},
		{"invalid_at_3", "abc" + string([]byte{0xC0, 0x80}), 3},  // overlong NUL
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := firstInvalidUTF8Offset(tc.in); got != tc.want {
				t.Errorf("firstInvalidUTF8Offset(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
