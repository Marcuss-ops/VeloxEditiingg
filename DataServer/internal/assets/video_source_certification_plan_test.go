package assets

import (
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestPlanVideoSourceCertificationTasksDeduplicatesByImmutableSource(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	sha := strings.Repeat("a", 64)
	plan := contract.CompiledRenderPlanV2{
		PlanVersion: 2, Output: contract.OutputContractV2{ProfileID: profile.ProfileID},
		Assets: []contract.AssetRefV2{
			{AssetID: "drive-a", SourceKind: "drive", DriveFileID: "file-a", Kind: "video", SHA256: sha, SizeBytes: 500},
			{AssetID: "drive-b", SourceKind: "drive", DriveFileID: "file-b", Kind: "video", SHA256: sha, SizeBytes: 500},
		},
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	service := &AssetService{videoWindowVerifier: newVideoWindowVerifier()}
	tasks, err := service.PlanVideoSourceCertificationTasks(string(encoded), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || len(tasks[0].AssetIDs) != 2 || tasks[0].Identity == "" {
		t.Fatalf("same immutable source produced duplicate certification tasks: %+v", tasks)
	}
	if tasks[0].Request.Source.SHA256 != sha || tasks[0].Request.Source.SizeBytes != 500 {
		t.Fatalf("certification task lost immutable identity: %+v", tasks[0])
	}
}

func TestPlanVideoSourceCertificationTasksUsesProvisionalDriveIdentity(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	plan := contract.CompiledRenderPlanV2{
		PlanVersion: 2, Output: contract.OutputContractV2{ProfileID: profile.ProfileID},
		Assets: []contract.AssetRefV2{{AssetID: "drive-a", SourceKind: "drive", DriveFileID: "file-a", Kind: "video", SizeBytes: 500}},
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	service := &AssetService{videoWindowVerifier: newVideoWindowVerifier()}
	tasks, err := service.PlanVideoSourceCertificationTasks(string(encoded), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || !strings.HasPrefix(tasks[0].Identity, "discovery:") || tasks[0].Request.Source.SHA256 != "" {
		t.Fatalf("unknown source did not get locator-scoped identity discovery: %+v", tasks)
	}
}
