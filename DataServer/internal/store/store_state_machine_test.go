package store

import (
	"errors"
	"testing"
)

// store_state_machine_test.go pins the canonical transition tables from
// store_state_machine.go. These tests are the single matrix for BOTH
// persistent machines — anyone extending a status vocabulary must extend
// the table here (and the enforcement points referenced in the module doc).

func TestValidateDeploymentTransition_Legal(t *testing.T) {
	legal := [][2]string{
		{DeployStatusPending, DeployStatusSucceeded},
		{DeployStatusPending, DeployStatusFailed},
		{DeployStatusPending, DeployStatusRolledBack},
	}
	for _, pair := range legal {
		if err := ValidateDeploymentTransition(pair[0], pair[1]); err != nil {
			t.Errorf("ValidateDeploymentTransition(%q, %q) = %v, want nil", pair[0], pair[1], err)
		}
	}
}

func TestValidateDeploymentTransition_Illegal(t *testing.T) {
	illegal := [][2]string{
		// No self-transition.
		{DeployStatusPending, DeployStatusPending},
		// Terminal states are immutable — no resurrections, not even into a
		// different terminal status.
		{DeployStatusSucceeded, DeployStatusSucceeded},
		{DeployStatusSucceeded, DeployStatusFailed},
		{DeployStatusSucceeded, DeployStatusPending},
		{DeployStatusFailed, DeployStatusFailed},
		{DeployStatusFailed, DeployStatusSucceeded},
		{DeployStatusFailed, DeployStatusPending},
		{DeployStatusRolledBack, DeployStatusRolledBack},
		{DeployStatusRolledBack, DeployStatusSucceeded},
		{DeployStatusRolledBack, DeployStatusFailed},
		{DeployStatusRolledBack, DeployStatusPending},
		// Unknown statuses are rejected on both sides.
		{"DRAINING", DeployStatusSucceeded},
		{DeployStatusPending, "BANANA"},
		{"", ""},
	}
	for _, pair := range illegal {
		err := ValidateDeploymentTransition(pair[0], pair[1])
		if err == nil {
			t.Errorf("ValidateDeploymentTransition(%q, %q) = nil, want ErrIllegalDeploymentTransition", pair[0], pair[1])
			continue
		}
		if !errors.Is(err, ErrIllegalDeploymentTransition) {
			t.Errorf("ValidateDeploymentTransition(%q, %q) = %v, want ErrIllegalDeploymentTransition", pair[0], pair[1], err)
		}
	}
}

func TestIsDeploymentStatusTerminal(t *testing.T) {
	terminal := []string{DeployStatusSucceeded, DeployStatusFailed, DeployStatusRolledBack}
	for _, status := range terminal {
		if !IsDeploymentStatusTerminal(status) {
			t.Errorf("IsDeploymentStatusTerminal(%q) = false, want true", status)
		}
	}
	nonTerminal := []string{DeployStatusPending, "DRAINING", "", "BANANA"}
	for _, status := range nonTerminal {
		if IsDeploymentStatusTerminal(status) {
			t.Errorf("IsDeploymentStatusTerminal(%q) = true, want false", status)
		}
	}
}

func TestValidateOperationTransition_Legal(t *testing.T) {
	legal := [][2]string{
		{OperationStatusQueued, OperationStatusRunning},
		{OperationStatusRunning, OperationStatusSucceeded},
		{OperationStatusRunning, OperationStatusFailed},
	}
	for _, pair := range legal {
		if err := ValidateOperationTransition(pair[0], pair[1]); err != nil {
			t.Errorf("ValidateOperationTransition(%q, %q) = %v, want nil", pair[0], pair[1], err)
		}
	}
}

func TestValidateOperationTransition_Illegal(t *testing.T) {
	illegal := [][2]string{
		// No self-transition.
		{OperationStatusQueued, OperationStatusQueued},
		{OperationStatusRunning, OperationStatusRunning},
		// QUEUED can only be claimed → RUNNING; a queued row can never jump
		// straight to a terminal status.
		{OperationStatusQueued, OperationStatusSucceeded},
		{OperationStatusQueued, OperationStatusFailed},
		// Terminal states are immutable.
		{OperationStatusSucceeded, OperationStatusSucceeded},
		{OperationStatusSucceeded, OperationStatusRunning},
		{OperationStatusFailed, OperationStatusFailed},
		{OperationStatusFailed, OperationStatusRunning},
		// Unknown statuses are rejected on both sides.
		{"PENDING", OperationStatusRunning},
		{OperationStatusRunning, "BANANA"},
		{"", ""},
	}
	for _, pair := range illegal {
		err := ValidateOperationTransition(pair[0], pair[1])
		if err == nil {
			t.Errorf("ValidateOperationTransition(%q, %q) = nil, want ErrIllegalOperationTransition", pair[0], pair[1])
			continue
		}
		if !errors.Is(err, ErrIllegalOperationTransition) {
			t.Errorf("ValidateOperationTransition(%q, %q) = %v, want ErrIllegalOperationTransition", pair[0], pair[1], err)
		}
	}
}

func TestIsOperationStatusTerminal(t *testing.T) {
	terminal := []string{OperationStatusSucceeded, OperationStatusFailed}
	for _, status := range terminal {
		if !IsOperationStatusTerminal(status) {
			t.Errorf("IsOperationStatusTerminal(%q) = false, want true", status)
		}
	}
	nonTerminal := []string{OperationStatusQueued, OperationStatusRunning, "PENDING", "", "BANANA"}
	for _, status := range nonTerminal {
		if IsOperationStatusTerminal(status) {
			t.Errorf("IsOperationStatusTerminal(%q) = true, want false", status)
		}
	}
}
