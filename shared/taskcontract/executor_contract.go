// Executor contracts
//
// A contract is the single declaration point for how an executor consumes a
// task. Before these contracts, required assets, prefetch eligibility and
// completion behavior were inferred independently from task payloads by the
// scheduler,
// the preparation gate and the worker. Those copies could disagree: a
// video.prepare.window.v1 task declares exactly one source, but generic
// payload walking could make it inherit every asset referenced elsewhere in
// the job. A contract makes the declaration explicit and testable.
//
// The registry is deliberately conservative for executors that have not been
// migrated yet: an unregistered executor is treated as prefetch-requiring.
// That preserves the existing strict claim fence while new executors migrate
// one at a time. It never silently grants an executor a weaker gate.
package taskcontract

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// CompletionKind describes the durable evidence an executor must produce
// before its attempt can be considered complete. It is intentionally a small
// closed vocabulary: adding a value requires the completion coordinator to
// understand it.
type CompletionKind string

const (
	// CompletionTaskResult is the ordinary TaskResult path for executors that
	// produce a normal task outcome.
	CompletionTaskResult CompletionKind = "task_result"
	// CompletionPreparedSegment is the video window-preparation path. Its
	// evidence is a certificate registered as an internal prepared fragment,
	// not a final_video artifact.
	CompletionPreparedSegment CompletionKind = "prepared_segment"
	// CompletionArtifactCommit is the artifact upload/commit path used by
	// assembly executors that publish the final video.
	CompletionArtifactCommit CompletionKind = "artifact_commit"
)

// RequiredAsset is one input the executor cannot run without. The key is the
// canonical logical identity used by placement affinity and prefetch plans;
// AssetID/SHA256/SizeBytes carry provider and integrity identity when known.
type RequiredAsset struct {
	Key       string
	AssetID   string
	SHA256    string
	SizeBytes int64
	Role      string
	SourceURI string
}

// TaskInput is the executor-visible identity and payload of one task. Payload
// is the decoded task_specs.payload_json object, never a second copy of the
// task row.
type TaskInput struct {
	ExecutorID      string
	ExecutorVersion int
	Payload         map[string]interface{}
}

// TaskInputFromJSON builds a TaskInput from persisted payload bytes.
func TaskInputFromJSON(executorID string, executorVersion int, payload []byte) (TaskInput, error) {
	spec := TaskInput{
		ExecutorID:      strings.TrimSpace(executorID),
		ExecutorVersion: executorVersion,
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &spec.Payload); err != nil {
			return TaskInput{}, fmt.Errorf("taskcontract: decode payload for %q: %w", spec.ExecutorID, err)
		}
	}
	return spec, nil
}

// DecodePayload decodes one top-level payload key into target. It fails when
// the key is absent so a missing declaration can never be mistaken for an
// empty-but-valid one.
func (s TaskInput) DecodePayload(key string, target interface{}) error {
	raw, ok := s.Payload[key]
	if !ok || raw == nil {
		return fmt.Errorf("taskcontract: payload key %q is missing", key)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("taskcontract: encode payload key %q: %w", key, err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		return fmt.Errorf("taskcontract: decode payload key %q: %w", key, err)
	}
	return nil
}

// TaskContract is the executor contract. Implementations must be pure: they
// receive a spec and declare facts, never perform I/O or mutate state.
type TaskContract interface {
	Validate(spec TaskInput) error
	RequiredAssets(spec TaskInput) ([]RequiredAsset, error)
	RequiresPrefetch(spec TaskInput) bool
	CompletionKind() CompletionKind
}

// Registry maps a concrete executor identity to its contract. It is safe for
// concurrent reads after construction; registration is expected at init.
type Registry struct {
	contracts map[string]TaskContract
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{contracts: make(map[string]TaskContract)}
}

func registryKey(executorID string, executorVersion int) string {
	return fmt.Sprintf("%s@%d", strings.TrimSpace(executorID), executorVersion)
}

// Register adds one contract. Duplicate identities, empty IDs, non-positive
// versions and nil contracts are rejected: silently overwriting one would
// make the authority ambiguous.
func (r *Registry) Register(executorID string, executorVersion int, taskContract TaskContract) error {
	if r == nil {
		return fmt.Errorf("taskcontract: nil registry")
	}
	if taskContract == nil {
		return fmt.Errorf("taskcontract: contract for %s@%d is nil", executorID, executorVersion)
	}
	if strings.TrimSpace(executorID) == "" || executorVersion <= 0 {
		return fmt.Errorf("taskcontract: executor identity %q@%d is invalid", executorID, executorVersion)
	}
	key := registryKey(executorID, executorVersion)
	if _, exists := r.contracts[key]; exists {
		return fmt.Errorf("taskcontract: contract for %s is already registered", key)
	}
	r.contracts[key] = taskContract
	return nil
}

// Lookup returns the contract for an exact executor identity.
func (r *Registry) Lookup(executorID string, executorVersion int) (TaskContract, bool) {
	if r == nil {
		return nil, false
	}
	taskContract, ok := r.contracts[registryKey(executorID, executorVersion)]
	return taskContract, ok
}

// Identities returns the registered executor identities in stable order.
func (r *Registry) Identities() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.contracts))
	for key := range r.contracts {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

var defaultRegistry = newDefaultRegistry()

func newDefaultRegistry() *Registry {
	registry := NewRegistry()
	register := func(executorID string, version int, taskContract TaskContract) {
		if err := registry.Register(executorID, version, taskContract); err != nil {
			panic(err)
		}
	}
	register(VideoPrepareWindowExecutorID, 1, videoPrepareWindowContract{})
	register(VideoAssemblyCopyExecutorID, 1, videoAssemblyContract{executorID: VideoAssemblyCopyExecutorID})
	register(VideoAssemblySliceExecutorID, 1, videoAssemblyContract{executorID: VideoAssemblySliceExecutorID})
	return registry
}

// Default returns the process-wide contract registry.
func Default() *Registry { return defaultRegistry }

// RequiredAssets resolves the authoritative asset declaration for a spec.
// The bool is false when no contract is registered, which tells callers to
// use their legacy extraction path instead of treating "unknown" as "none".
func (r *Registry) RequiredAssets(spec TaskInput) ([]RequiredAsset, bool, error) {
	taskContract, ok := r.Lookup(spec.ExecutorID, spec.ExecutorVersion)
	if !ok {
		return nil, false, nil
	}
	assets, err := taskContract.RequiredAssets(spec)
	if err != nil {
		return nil, true, err
	}
	return canonicalRequiredAssets(assets), true, nil
}

// RequiresPrefetch reports whether the preparation gate applies to a spec.
// Unregistered executors are treated as requiring prefetch: the legacy strict
// fence is preserved until an executor declares otherwise.
func (r *Registry) RequiresPrefetch(spec TaskInput) bool {
	taskContract, ok := r.Lookup(spec.ExecutorID, spec.ExecutorVersion)
	if !ok {
		return true
	}
	return taskContract.RequiresPrefetch(spec)
}

func canonicalRequiredAssets(in []RequiredAsset) []RequiredAsset {
	byKey := make(map[string]RequiredAsset, len(in))
	for _, asset := range in {
		asset.Key = strings.TrimSpace(asset.Key)
		asset.AssetID = strings.TrimSpace(asset.AssetID)
		asset.SHA256 = strings.TrimSpace(asset.SHA256)
		if asset.Key == "" {
			continue
		}
		if previous, exists := byKey[asset.Key]; exists {
			// Keep the more complete declaration. Conflicting content
			// identity is a contract bug, but the key set is what the
			// scheduler consumes, so prefer the entry with a SHA.
			if previous.SHA256 != "" && asset.SHA256 == "" {
				continue
			}
		}
		byKey[asset.Key] = asset
	}
	out := make([]RequiredAsset, 0, len(byKey))
	for _, asset := range byKey {
		out = append(out, asset)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
