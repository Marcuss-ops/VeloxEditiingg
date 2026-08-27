package worker

import "velox-worker-agent/internal/prefetch"

// admissionAdapter wraps *ResourceAdmissionController to satisfy the
// prefetch.ResourceAdmissionController interface. This avoids circular
// imports while allowing the prefetch scheduler to check RSS admission
// before each download.
type admissionAdapter struct {
	rac *ResourceAdmissionController
}

func newAdmissionAdapter(rac *ResourceAdmissionController) prefetch.ResourceAdmissionController {
	if rac == nil {
		return nil
	}
	return &admissionAdapter{rac: rac}
}

func (a *admissionAdapter) CanAdmit(category prefetch.AdmissionCategory) prefetch.AdmissionDecision {
	claim := ResourceClaim{Kind: kindFromPrefetch(category)}
	decision := a.rac.CanAdmit(claim)
	return decisionFromWorker(decision)
}

func (a *admissionAdapter) RecordAdmissionResult(category prefetch.AdmissionCategory, admitted bool) {
	claim := ResourceClaim{Kind: kindFromPrefetch(category)}
	a.rac.RecordAdmissionResult(claim, admitted)
}

func kindFromPrefetch(category prefetch.AdmissionCategory) ResourceKind {
	switch category {
	case prefetch.AdmissionPrefetch:
		return ResourcePrefetch
	case prefetch.AdmissionPublish:
		return ResourcePublish
	case prefetch.AdmissionRender:
		return ResourceRender
	default:
		return ResourcePrefetch
	}
}

func decisionFromWorker(d AdmissionDecision) prefetch.AdmissionDecision {
	switch d {
	case Admit:
		return prefetch.AdmissionAdmit
	case RejectMemory:
		return prefetch.AdmissionRejectMemory
	case RejectStopped:
		return prefetch.AdmissionRejectStopped
	default:
		return prefetch.AdmissionRejectMemory
	}
}
