module velox-worker-agent

go 1.25.0

require (
	github.com/jdkato/prose/v2 v2.0.0
	github.com/stretchr/testify v1.11.1
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.11
	velox-shared v0.0.0
)

// Required for standalone builds OUTSIDE the go.work workspace resolution
// (i.e. the Docker builder stage in RemoteCodex/native/worker-agent-go/Dockerfile).
// The workspace 'replace velox-shared v0.0.0 => ./shared' in /go.work only
// applies when building with `go build` from a parent directory that has
// the workspace file in scope. The Docker builder stage enters the module
// directory directly (WORKDIR RemoteCodex/native/worker-agent-go) and has
// no ambient go.work, so the module-level replace below is what makes the
// build resolve to the canonical `shared/` tree at the repo root.
//
// Path is relative to this module directory (RemoteCodex/native/worker-agent-go):
//   ../../../shared  ==  <repo-root>/shared
//
// CI workflow .github/workflows/master-image.yml hard-fails on the absence
// of this clause; see the failing-check definition there for rationale.
replace velox-shared v0.0.0 => ../../../shared


require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/deckarep/golang-set v1.7.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/mingrammer/commonregex v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260618152121-87f3d3e198d3 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/neurosnap/sentences.v1 v1.0.6 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
