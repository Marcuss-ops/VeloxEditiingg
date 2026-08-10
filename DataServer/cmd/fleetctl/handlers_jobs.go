// handlers_jobs.go — job diagnostics and production doctor handlers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func runJob(client *fleetClient, args []string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "job requires inspect, metrics, or watch plus a job_id"))
		return ExitMisuse
	}
	switch args[0] {
	case "inspect":
		return runJobInspect(client, args[1])
	case "metrics":
		return runJobMetrics(client, args[1])
	case "watch":
		return runJobWatch(client, args[1])
	default:
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "unknown job command %q", args[0]))
		return ExitMisuse
	}
}
func runJobInspect(client *fleetClient, jobID string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response map[string]any
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/jobs/"+jobID, nil, &response)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != httpOK {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "GET /api/v1/admin/jobs/%s status=%d", jobID, status))
		return ec
	}
	bs, _ := json.MarshalIndent(response, "", "  ")
	fmt.Println(string(bs))
	return ExitOK
}
func runJobMetrics(client *fleetClient, jobID string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response map[string]any
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/jobs/"+jobID+"/metrics", nil, &response)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != httpOK {
		ec := MapHTTPStatusToOpExit(status)
		fmt.Fprintln(os.Stderr, fmtExit(ec, "GET job metrics status=%d", status))
		return ec
	}
	bs, _ := json.MarshalIndent(response, "", "  ")
	fmt.Println(string(bs))
	return ExitOK
}
func runJobWatch(client *fleetClient, jobID string) int {
	seen := map[string]bool{}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		var response struct {
			Status string `json:"status"`
			Job    struct {
				Status string `json:"status"`
			} `json:"job"`
			Events []struct {
				Timestamp string         `json:"timestamp"`
				Event     string         `json:"event"`
				Payload   map[string]any `json:"payload"`
			} `json:"events"`
		}
		status, err := client.doJSON(ctx, "GET", "/api/v1/admin/jobs/"+jobID+"/events", nil, &response)
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
			return ExitUnexpected
		}
		if status != httpOK {
			ec := MapHTTPStatusToOpExit(status)
			fmt.Fprintln(os.Stderr, fmtExit(ec, "GET job events status=%d", status))
			return ec
		}
		for _, event := range response.Events {
			key := event.Timestamp + "\x00" + event.Event
			if seen[key] {
				continue
			}
			seen[key] = true
			fmt.Printf("%s %-24s", event.Timestamp, event.Event)
			if len(event.Payload) > 0 {
				payload, _ := json.Marshal(event.Payload)
				fmt.Printf(" %s", payload)
			}
			fmt.Println()
		}
		terminalStatus := response.Status
		if terminalStatus == "" {
			terminalStatus = response.Job.Status
		}
		if terminalStatus == "SUCCEEDED" || terminalStatus == "FAILED" || terminalStatus == "CANCELLED" {
			return ExitOK
		}
		time.Sleep(2 * time.Second)
	}
}
func runDoctor(client *fleetClient, args []string) int {
	production := false
	for _, arg := range args {
		if arg == "--production" {
			production = true
		}
	}
	if !production {
		fmt.Fprintln(os.Stderr, fmtExit(ExitMisuse, "doctor requires --production"))
		return ExitMisuse
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var response struct {
		Healthy bool `json:"healthy"`
		Checks  []struct {
			WorkerID string `json:"worker_id"`
			Name     string `json:"name"`
			Check    string `json:"check"`
			Status   string `json:"status"`
			Detail   string `json:"detail"`
		} `json:"checks"`
	}
	status, err := client.doJSON(ctx, "GET", "/api/v1/admin/doctor/production", nil, &response)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmtExit(ExitUnexpected, "%v", err))
		return ExitUnexpected
	}
	if status != httpOK {
		fmt.Fprintln(os.Stderr, fmtExit(MapHTTPStatusToOpExit(status), "GET production doctor status=%d", status))
		return MapHTTPStatusToOpExit(status)
	}
	for _, check := range response.Checks {
		who := check.WorkerID
		if check.Name != "" {
			who = check.Name + " (" + who + ")"
		}
		fmt.Printf("%-42s %-12s %-8s %s\n", who, check.Check, check.Status, check.Detail)
	}
	if !response.Healthy {
		return ExitUnexpected
	}
	return ExitOK
}

const httpOK = 200

// runDrain — POST /api/v1/admin/workers/{id}/drain;
// polls /admin/operations/{op_id} until terminal.
