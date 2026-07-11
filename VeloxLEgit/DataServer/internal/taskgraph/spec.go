// Package taskgraph provides the canonical Task domain model for distributed
// rendering. A Task is the unit of work assigned to a single worker execution.
//
// TaskSpec is re-exported from velox-shared/taskcontract for a single source
// of truth across master and worker.
package taskgraph

import "velox-shared/taskcontract"

// TaskSpec is the canonical task specification, re-exported from the shared
// contract package. Both master and worker import the same definition.
type TaskSpec = taskcontract.TaskSpec

// SpecVersion is the current canonical task spec version, re-exported from
// the shared contract.
const SpecVersion = taskcontract.SpecVersion
