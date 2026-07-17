package remoteengine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// GenerateSimpleScript generates a single script from a topic.
func (c *Client) GenerateSimpleScript(ctx context.Context, req SimpleScriptRequest) (*SimpleScriptResponse, error) {
	if !c.IsConfigured() {
		return nil, ErrNotConfigured
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorValidation,
			Code:    "MARSHAL",
			Message: fmt.Sprintf("failed to marshal request: %v", err),
			Cause:   err,
		}
	}

	var resp *SimpleScriptResponse

	retryErr := c.withRetry(ctx, func(attempt int) error {
		r, e := c.doSimpleScriptRequest(ctx, body)
		if e != nil {
			return e
		}
		resp = r
		return nil
	})

	if retryErr != nil {
		return nil, retryErr
	}
	return resp, nil
}

func (c *Client) doSimpleScriptRequest(ctx context.Context, body []byte) (*SimpleScriptResponse, error) {
	httpReq, err := c.newRequest(ctx, http.MethodPost, "/api/script-simple", body, "")
	if err != nil {
		return nil, err
	}

	respBody, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}

	var result SimpleScriptResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	return &result, nil
}

// GenerateBatchScripts generates multiple scripts from topics.
func (c *Client) GenerateBatchScripts(ctx context.Context, req BatchScriptRequest) (*BatchScriptResponse, error) {
	if !c.IsConfigured() {
		return nil, ErrNotConfigured
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, &RemoteError{
			Class:   RemoteErrorValidation,
			Code:    "MARSHAL",
			Message: fmt.Sprintf("failed to marshal request: %v", err),
			Cause:   err,
		}
	}

	var resp *BatchScriptResponse

	retryErr := c.withRetry(ctx, func(attempt int) error {
		r, e := c.doBatchScriptRequest(ctx, body)
		if e != nil {
			return e
		}
		resp = r
		return nil
	})

	if retryErr != nil {
		return nil, retryErr
	}
	return resp, nil
}

func (c *Client) doBatchScriptRequest(ctx context.Context, body []byte) (*BatchScriptResponse, error) {
	httpReq, err := c.newRequest(ctx, http.MethodPost, "/api/script-multiple", body, "")
	if err != nil {
		return nil, err
	}

	respBody, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}

	var result BatchScriptResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	return &result, nil
}
