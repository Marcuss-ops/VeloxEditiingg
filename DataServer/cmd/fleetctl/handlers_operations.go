// handlers_operations.go — operation listing and worker readiness polling.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type operationsListResponse struct {
	Count      int                `json:"count"`
	Operations []operationListRow `json:"operations"`
}

type operationListRow struct {
	OperationID  string `json:"operation_id"`
	WorkerID     string `json:"worker_id"`
	Op           string `json:"op"`
	RequestedBy  string `json:"requested_by"`
	Reason       string `json:"reason"`
	Status       string `json:"status"`
	QueuedAt     string `json:"queued_at"`
	StartedAt    string `json:"started_at,omitempty"`
	FinishedAt   string `json:"finished_at,omitempty"`
	Payload      string `json:"payload,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// runOperations lists the canonical fleet_operations audit envelope. Positional
// filters preserve the shell contract: operations [worker_id] [status].
func runOperations(client *fleetClient, args []string) int {
	filters, err := parseOperationsArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
		return ExitMisuse
	}
	path := "/api/v1/admin/operations"
	query := url.Values{}
	if filters.workerID != "" {
		query.Set("worker_id", filters.workerID)
	}
	if filters.status != "" {
		query.Set("status", filters.status)
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response := operationsListResponse{}
	status, requestErr := client.doJSON(ctx, "GET", path, nil, &response)
	if requestErr != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", requestErr))
		return ExitUnexpected
	}
	if status != 200 {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "GET /admin/operations status=%d", status))
		return ec
	}
	if response.Operations == nil {
		response.Operations = []operationListRow{}
	}
	if response.Count != len(response.Operations) {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "operations response count=%d does not match len(operations)=%d", response.Count, len(response.Operations)))
		return ExitUnexpected
	}
	encoded, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "encode operations response: %v", err))
		return ExitUnexpected
	}
	fmt.Println(string(encoded))
	return ExitOK
}

type operationsFilters struct {
	workerID string
	status   string
}

func parseOperationsArgs(args []string) (operationsFilters, error) {
	var filters operationsFilters
	var positional []string
	var workerFlag, statusFlag bool
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--worker-id" || args[i] == "--status":
			if i+1 >= len(args) {
				return filters, fmt.Errorf("%s requires a value", args[i])
			}
			if args[i] == "--worker-id" {
				if workerFlag {
					return filters, errors.New("worker_id filter specified more than once")
				}
				filters.workerID, workerFlag = args[i+1], true
			} else {
				if statusFlag {
					return filters, errors.New("status filter specified more than once")
				}
				filters.status, statusFlag = args[i+1], true
			}
			i++
		case strings.HasPrefix(args[i], "--worker-id="):
			if workerFlag {
				return filters, errors.New("worker_id filter specified more than once")
			}
			filters.workerID, workerFlag = strings.TrimPrefix(args[i], "--worker-id="), true
		case strings.HasPrefix(args[i], "--status="):
			if statusFlag {
				return filters, errors.New("status filter specified more than once")
			}
			filters.status, statusFlag = strings.TrimPrefix(args[i], "--status="), true
		case strings.HasPrefix(args[i], "--master=") || strings.HasPrefix(args[i], "--token-file=") || args[i] == "--verbose":
		case args[i] == "--master" || args[i] == "--token-file":
			if i+1 >= len(args) {
				return filters, fmt.Errorf("%s requires a value", args[i])
			}
			i++
		case strings.HasPrefix(args[i], "-"):
			return filters, fmt.Errorf("unknown operations option %q", args[i])
		default:
			positional = append(positional, args[i])
		}
	}
	if len(positional) > 2 {
		return filters, errors.New("operations accepts worker_id and optional status")
	}
	if len(positional) > 0 {
		if workerFlag {
			return filters, errors.New("worker_id filter cannot be provided both positionally and by flag")
		}
		filters.workerID = positional[0]
	}
	if len(positional) > 1 {
		if statusFlag {
			return filters, errors.New("status filter cannot be provided both positionally and by flag")
		}
		filters.status = positional[1]
	}
	return filters, nil
}

// runWaitReady polls a worker's canonical WorkerCard until it satisfies the
// readiness contract used by the legacy shell command.
func runWaitReady(client *fleetClient, args []string) int {
	workerID, expected, timeout, interval, err := parseWaitReadyArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "%v", err))
		return ExitMisuse
	}
	return runWaitReadyWithInterval(client, workerID, expected, timeout, interval)
}

func parseWaitReadyArgs(args []string) (workerID, expected string, timeout, interval time.Duration, err error) {
	timeout = envSeconds("FLEETCTL_READY_TIMEOUT_SECONDS", 180)
	interval = envSeconds("FLEETCTL_READY_POLL_SECONDS", 5)
	var positional []string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--digest" || args[i] == "--timeout" || args[i] == "--poll":
			if i+1 >= len(args) {
				return "", "", 0, 0, fmt.Errorf("%s requires a value", args[i])
			}
			value := args[i+1]
			switch args[i] {
			case "--digest":
				expected = value
			case "--timeout":
				timeout, err = parseSeconds(value, "--timeout")
			case "--poll":
				interval, err = parseSeconds(value, "--poll")
			}
			if err != nil {
				return "", "", 0, 0, err
			}
			i++
		case strings.HasPrefix(args[i], "--digest="):
			expected = strings.TrimPrefix(args[i], "--digest=")
		case strings.HasPrefix(args[i], "--timeout="):
			timeout, err = parseSeconds(strings.TrimPrefix(args[i], "--timeout="), "--timeout")
		case strings.HasPrefix(args[i], "--poll="):
			interval, err = parseSeconds(strings.TrimPrefix(args[i], "--poll="), "--poll")
		case strings.HasPrefix(args[i], "--master=") || strings.HasPrefix(args[i], "--token-file=") || args[i] == "--verbose":
		case args[i] == "--master" || args[i] == "--token-file":
			if i+1 >= len(args) {
				return "", "", 0, 0, fmt.Errorf("%s requires a value", args[i])
			}
			i++
		case strings.HasPrefix(args[i], "-"):
			return "", "", 0, 0, fmt.Errorf("unknown wait-ready option %q", args[i])
		default:
			positional = append(positional, args[i])
		}
	}
	if len(positional) != 1 {
		return "", "", 0, 0, errors.New("wait-ready requires exactly one worker_id")
	}
	if timeout <= 0 || interval < 0 {
		return "", "", 0, 0, errors.New("wait-ready timeout must be positive and poll interval cannot be negative")
	}
	if expected != "" && digestFromRef(expected) == "" {
		return "", "", 0, 0, fmt.Errorf("readiness digest %q is not immutable", expected)
	}
	return positional[0], expected, timeout, interval, nil
}

func runWaitReadyWithInterval(client *fleetClient, workerID, expected string, timeout, interval time.Duration) int {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	expectedDigest := digestFromRef(expected)
	for {
		card := workerCardResponse{}
		status, err := client.doJSON(ctx, "GET", workerReadPath(workerID), nil, &card)
		if err != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
			return ExitUnexpected
		}
		if status != 200 {
			ec := MapHTTPStatusToOpExit(status)
			fmt.Fprintln(os.Stderr, fmtExit(ec, "GET /admin/workers/%s status=%d", workerID, status))
			return ec
		}
		ready, connection, health, actualDigest := workerReadyState(card)
		fmt.Fprintf(os.Stderr, "fleetctl: worker=%s connection=%s health=%s readiness=%s digest=%s\n", workerID, connection, health, ready, displayValue(actualDigest))
		if ready == "ok" && connection == "CONNECTED" && health == "HEALTHY" &&
			(expectedDigest == "" || digestFromRef(actualDigest) == expectedDigest) {
			fmt.Fprintf(os.Stderr, "fleetctl: worker=%s READY\n", workerID)
			return ExitOK
		}
		if !waitPollingInterval(ctx, interval) {
			fmt.Fprintf(os.Stderr, "fleetctl: worker=%s did not become READY (connection=%s health=%s readiness=%s digest=%s expected=%s)\n", workerID, connection, health, ready, displayValue(actualDigest), displayValue(expectedDigest))
			return ExitUnexpected
		}
	}
}

func workerReadyState(card workerCardResponse) (readiness, connection, health, imageDigest string) {
	if value, ok := card["readiness"].(map[string]any); ok {
		readiness, _ = value["status"].(string)
	}
	connection, _ = card["connection_state"].(string)
	if connection == "" {
		connection, _ = card["status"].(string)
	}
	health, _ = card["health"].(string)
	if health == "" {
		health, _ = card["health_state"].(string)
	}
	imageDigest, _ = card["image_digest"].(string)
	if imageDigest == "" {
		imageDigest, _ = card["digest"].(string)
	}
	return
}

func envSeconds(name string, fallback int) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return time.Duration(fallback) * time.Second
	}
	parsed, err := parseSeconds(value, name)
	if err != nil {
		return time.Duration(fallback) * time.Second
	}
	return parsed
}

func parseSeconds(value, flagName string) (time.Duration, error) {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer seconds value", flagName)
	}
	return time.Duration(seconds) * time.Second, nil
}

func displayValue(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
