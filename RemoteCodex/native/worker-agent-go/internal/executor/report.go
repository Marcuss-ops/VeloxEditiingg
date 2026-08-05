// Package executor — capability report builder.
//
// Derives the worker's hello / heartbeat capability payload
// directly from Registry.Descriptors(). This file is the SINGLE
// place where the mapping lives: every hello and every heartbeat on
// the wire end up here, so a change in executor shape ONLY requires
// touching one function and one Descriptor field.
package executor

import "velox-shared/controltransport"

// BuildCapabilityReport produces the canonical, deterministic
// capability snapshot for the worker. Ordering is stable because
// Registry.Descriptors() returns Descriptors sorted by (ID, Version).
//
// Invariants:
//   - Host and Executors are non-nil.
//   - SchemaVersion is controltransport.CapabilitySchemaVersion.
//   - Nil registry PANICS — this is consistent with WithRegistry(nil)
//     panic-on-nil. Empty registry is valid and yields an empty
//     Executors slice; callers who want an empty registry should
//     pass executor.NewRegistry() explicitly.
//
// Worker.buildHello and Worker.sendHeartbeat call this and pass the
// AsMap result into the controltransport envelope, so the master
// sees exactly one capability schema regardless of code path.
func BuildCapabilityReport(reg *Registry, host controltransport.HostInfo) controltransport.CapabilityReport {
	if reg == nil {
		panic("executor.BuildCapabilityReport: registry must not be nil — pass executor.NewRegistry() for an empty default")
	}
	report := controltransport.CapabilityReport{
		SchemaVersion: controltransport.CapabilitySchemaVersion,
		Executors:     []controltransport.ExecutorCapability{},
		Host:          host,
	}
	for _, d := range reg.Descriptors() {
		report.Executors = append(report.Executors, executorCapabilityFromDescriptor(d))
	}
	return report
}

// executorCapabilityFromDescriptor maps one executor.Descriptor into the
// wire-facing ExecutorCapability shape. The mapping is intentionally
// narrow — only fields the master needs to make scheduling decisions are
// exported. Internal-only fields (InputTypes) stay internal.
func executorCapabilityFromDescriptor(d Descriptor) controltransport.ExecutorCapability {
	outputs := d.OutputTypes
	if outputs == nil {
		outputs = []string{}
	}
	return controltransport.ExecutorCapability{
		ID:            d.ID,
		Version:       d.Version,
		ResourceClass: string(d.ResourceClass),
		TemporalMode:  string(d.TemporalMode),
		Deterministic: d.Deterministic,
		Cacheable:     d.Cacheable,
		SupportsAlpha: d.SupportsAlpha,
		OutputTypes:   outputs,
	}
}
