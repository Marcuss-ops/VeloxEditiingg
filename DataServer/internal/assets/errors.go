package assets

import (
	"net/http"
	"strings"
)

const (
	// VoiceoverAssetUnavailableCode is returned when a voiceover reference cannot be acquired.
	VoiceoverAssetUnavailableCode = "VOICEOVER_ASSET_UNAVAILABLE"
)

// AcquisitionError is a structured failure returned by the asset bridge.
type AcquisitionError struct {
	Code       string
	Field      string
	Message    string
	SourceType string
	Cause      error
}

func (e *AcquisitionError) Error() string {
	if e == nil {
		return ""
	}
	parts := []string{e.Code}
	if e.Field != "" {
		parts = append(parts, e.Field)
	}
	if e.Message != "" {
		parts = append(parts, e.Message)
	}
	return strings.Join(parts, ": ")
}

func (e *AcquisitionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// StatusCode returns the HTTP status that best matches the acquisition error.
func (e *AcquisitionError) StatusCode() int {
	if e == nil {
		return http.StatusUnprocessableEntity
	}
	return http.StatusUnprocessableEntity
}

func newAcquisitionError(field, sourceType, message string, cause error) *AcquisitionError {
	if strings.TrimSpace(message) == "" && cause != nil {
		message = cause.Error()
	}
	return &AcquisitionError{
		Code:       VoiceoverAssetUnavailableCode,
		Field:      field,
		Message:    message,
		SourceType: sourceType,
		Cause:      cause,
	}
}

// AsAcquisitionError unwraps a structured asset error from an error chain.
func AsAcquisitionError(err error) (*AcquisitionError, bool) {
	if err == nil {
		return nil, false
	}
	var assetErr *AcquisitionError
	if ok := errorAs(err, &assetErr); ok && assetErr != nil {
		return assetErr, true
	}
	return nil, false
}

// errorAs mirrors errors.As without importing the entire package in every file.
func errorAs(err error, target interface{}) bool {
	switch t := target.(type) {
	case **AcquisitionError:
		for err != nil {
			if ae, ok := err.(*AcquisitionError); ok {
				*t = ae
				return true
			}
			type unwrapper interface{ Unwrap() error }
			if u, ok := err.(unwrapper); ok {
				err = u.Unwrap()
				continue
			}
			break
		}
	}
	return false
}


