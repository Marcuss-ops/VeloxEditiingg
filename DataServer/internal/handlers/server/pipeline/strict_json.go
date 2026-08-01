package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// MaxStrictJSONBodyBytes bounds request bodies decoded by the external job
// intake endpoints. The limit prevents malformed clients from forcing an
// unbounded io.ReadAll allocation before semantic validation runs.
const MaxStrictJSONBodyBytes = 4 << 20

// decodeStrictJSON enforces the API boundary contract shared by single and
// batch submissions: UTF-8 input, one JSON value, and no unknown fields.
func decodeStrictJSON(body io.Reader, destination any) error {
	raw, err := io.ReadAll(io.LimitReader(body, MaxStrictJSONBodyBytes+1))
	if err != nil {
		return fmt.Errorf("read request body: %w", err)
	}
	if len(raw) > MaxStrictJSONBodyBytes {
		// Consume the remainder when the reader is an HTTP request body so
		// rejected oversized requests do not leave bytes on a keep-alive
		// connection. Ignore the drain error: the size violation is the
		// actionable response and the body is already being rejected.
		_, _ = io.Copy(io.Discard, body)
		return fmt.Errorf("request body exceeds %d bytes", MaxStrictJSONBodyBytes)
	}
	if !utf8.Valid(raw) {
		return errors.New("request body must be valid UTF-8")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain exactly one JSON value")
		}
		return err
	}
	return nil
}
