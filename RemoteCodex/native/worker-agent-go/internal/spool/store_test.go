// Tests for worker_output_spool (Artifact Commit Protocol Phase 3.1).
//
// Coverage targets:
//   - Open + Close lifecycle (in-memory DB).
//   - Insert + dedupe on (task_id, attempt_id, worker_spool_key).
//   - Get + ListByStatus + ListByAttempt + ListResumeCandidates.
//   - All 8 lifecycle transitions (MarkReady .. MarkCleaned) including CAS conflicts.
//   - MarkReject terminal-state guard.
//   - Status.IsValid closed vocabulary.
//   - Delete is reserved for cleanup tools.
package spool

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// helper: turn a fresh in-memory DB into a Store. Tests get a clean
// schema each time (dat name == spool-N so even :memory: DSNs do not
// share tables across test functions on shared-cache connections).
func newInMemoryTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(":memory:")
	if err != nil {
		t.Fatalf("spool.Open(:memory:) → %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// helper: instantiate a baseline entry in StatusRendering so transition
// tests start from a known state.
func mustInsertBasic(t *testing.T, s *Store, keySuffix string) *SpoolEntry {
	t.Helper()
	in := SpoolEntry{
		TaskID:         "task-" + keySuffix,
		AttemptID:      "attempt-" + keySuffix,
		WorkerSpoolKey: "wsk-" + keySuffix,
		LocalPath:      "/var/scratch/" + keySuffix + ".mp4",
	}
	out, err := s.Insert(context.Background(), in)
	if err != nil {
		t.Fatalf("spool.Insert(%s) → %v", keySuffix, err)
	}
	if out.Status != StatusRendering {
		t.Fatalf("Insert: expected status RENDERING, got %q", out.Status)
	}
	if out.SpoolID == "" {
		t.Fatalf("Insert: SpoolID must be assigned")
	}
	if out.CreatedAt.IsZero() || out.UpdatedAt.IsZero() {
		t.Fatalf("Insert: timestamps must be stamped")
	}
	return out
}

func TestStatus_IsValid_ClosedVocabulary(t *testing.T) {
	want := map[Status]bool{
		StatusRendering:     true,
		StatusOutputReady:   true,
		StatusUploadPending: true,
		StatusUploading:     true,
		StatusUploaded:      true,
		StatusCommitted:     true,
		StatusRejected:      true,
		StatusCleaned:       true,
		"INVENTED":          false,
		"":                  false,
	}
	for s, expected := range want {
		if got := s.IsValid(); got != expected {
			t.Errorf("Status(%q).IsValid() = %v; want %v", s, got, expected)
		}
	}
	// AllStatuses must list 8 entries in lifecycle order.
	if len(AllStatuses) != 8 {
		t.Fatalf("AllStatuses length = %d; want 8", len(AllStatuses))
	}
	wantOrder := []Status{
		StatusRendering, StatusOutputReady, StatusUploadPending, StatusUploading,
		StatusUploaded, StatusCommitted, StatusRejected, StatusCleaned,
	}
	for i, s := range AllStatuses {
		if s != wantOrder[i] {
			t.Errorf("AllStatuses[%d] = %q; want %q", i, s, wantOrder[i])
		}
	}
}

func TestOpen_And_Close_Roundtrip(t *testing.T) {
	// File-backed round-trip ensures schemaDDL is replayable on a
	// real path (not just :memory:).
	path := filepath.Join(t.TempDir(), "spool.db")
	s1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open(path=%q) → %v", path, err)
	}
	defer s1.Close()
	if _, err := s1.Insert(context.Background(), SpoolEntry{
		TaskID: "T1", AttemptID: "A1", WorkerSpoolKey: "K1",
	}); err != nil {
		t.Fatalf("first Open Insert → %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("first Close → %v", err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen Open(path=%q) → %v", path, err)
	}
	defer s2.Close()
	rows, err := s2.ListByAttempt(context.Background(), "T1", "A1")
	if err != nil {
		t.Fatalf("reopen ListByAttempt → %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("reopen: expected 1 row, got %d", len(rows))
	}
}

func TestInsert_DuplicateKey_TaskAttemptSpoolKeyErrors(t *testing.T) {
	s := newInMemoryTestStore(t)
	common := func(i int) SpoolEntry {
		return SpoolEntry{
			TaskID: "TX", AttemptID: "AX", WorkerSpoolKey: "WKX",
			LocalPath: "/var/scratch/" + string(rune('a'+i)) + ".mp4",
		}
	}
	if _, err := s.Insert(context.Background(), common(0)); err != nil {
		t.Fatalf("first Insert → %v", err)
	}
	_, err := s.Insert(context.Background(), common(1))
	if err == nil {
		t.Fatalf("second Insert: expected ErrDuplicateSpool, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("second Insert: expected error to mention duplicate, got %v", err)
	}
	// Allowed: same (task,attempt) with a different worker_spool_key is
	// NOT a dup — different logical output for the same attempt.
	if _, err := s.Insert(context.Background(), SpoolEntry{
		TaskID: "TX", AttemptID: "AX", WorkerSpoolKey: "WKX-OTHER",
	}); err != nil {
		t.Fatalf("Insert(same T+A, distinct spool key) → %v", err)
	}
}

func TestInsert_MissingRequiredFields_Errors(t *testing.T) {
	s := newInMemoryTestStore(t)
	cases := []SpoolEntry{
		{TaskID: "", AttemptID: "A1", WorkerSpoolKey: "K1"},
		{TaskID: "T1", AttemptID: "", WorkerSpoolKey: "K1"},
		{TaskID: "T1", AttemptID: "A1", WorkerSpoolKey: ""},
		{TaskID: "T1", AttemptID: "A1", WorkerSpoolKey: "K1", Status: "BOGUS"},
	}
	for i, in := range cases {
		_, err := s.Insert(context.Background(), in)
		if err == nil {
			t.Errorf("case %d: expected error, got nil", i)
		}
	}
}

func TestGet_NotFound_ErrNotFound(t *testing.T) {
	s := newInMemoryTestStore(t)
	_, err := s.Get(context.Background(), "missing")
	if err != ErrNotFound {
		t.Fatalf("Get(missing) → %v; want ErrNotFound", err)
	}
}

func TestListByStatus_And_ListByAttempt_Ordering(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	a1 := mustInsertBasic(t, s, "A1")
	a2 := mustInsertBasic(t, s, "A2")
	// Bump A1 → OUTPUT_READY.
	if err := s.MarkReady(ctx, a1.SpoolID, strings.Repeat("a", 64), 1024); err != nil {
		t.Fatalf("MarkReady → %v", err)
	}
	readyRows, err := s.ListByStatus(ctx, StatusOutputReady)
	if err != nil {
		t.Fatalf("ListByStatus(OUTPUT_READY) → %v", err)
	}
	if len(readyRows) != 1 || readyRows[0].SpoolID != a1.SpoolID {
		t.Fatalf("ListByStatus(OUTPUT_READY) returned %d rows, want 1 (A1)", len(readyRows))
	}
	// ListByAttempt must surface both rows for A1's task with same
	// attempt; we re-use A1's (T, A) but a different spool_key for a
	// second physical output.
	if _, err := s.Insert(ctx, SpoolEntry{
		TaskID: a1.TaskID, AttemptID: a1.AttemptID, WorkerSpoolKey: "alt",
	}); err != nil {
		t.Fatalf("Insert(second on A1's attempt) → %v", err)
	}
	rows, err := s.ListByAttempt(ctx, a1.TaskID, a1.AttemptID)
	if err != nil {
		t.Fatalf("ListByAttempt → %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("ListByAttempt: got %d rows, want 2", len(rows))
	}
	if rows[0].CreatedAt.After(rows[1].CreatedAt) {
		t.Fatalf("ListByAttempt: not in CreatedAt-ascending order")
	}
	// Sanity: A2 is in RENDERING, not part of A1's attempt list.
	if _, err := s.Get(ctx, a2.SpoolID); err != nil {
		t.Fatalf("Get(a2) → %v", err)
	}
}

func TestListResumeCandidates_OnlyMidUploadStates(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	a1 := mustInsertBasic(t, s, "RA")
	a2 := mustInsertBasic(t, s, "RB")
	a3 := mustInsertBasic(t, s, "RC")
	a4 := mustInsertBasic(t, s, "RD")
	a5 := mustInsertBasic(t, s, "RE")

	// A1 → OUTPUT_READY (resume candidate)
	if err := s.MarkReady(ctx, a1.SpoolID, strings.Repeat("1", 64), 10); err != nil {
		t.Fatalf("MarkReady → %v", err)
	}
	// A2 → UPLOAD_PENDING
	_ = s.MarkReady(ctx, a2.SpoolID, strings.Repeat("2", 64), 11)
	uploadID := "up_" + a2.SpoolID
	if err := s.MarkUploadPending(ctx, a2.SpoolID, uploadID); err != nil {
		t.Fatalf("MarkUploadPending → %v", err)
	}
	// A3 → UPLOADING
	_ = s.MarkReady(ctx, a3.SpoolID, strings.Repeat("3", 64), 12)
	_ = s.MarkUploadPending(ctx, a3.SpoolID, "up_a3")
	if err := s.MarkUploading(ctx, a3.SpoolID, 0); err != nil {
		t.Fatalf("MarkUploading → %v", err)
	}
	// A4 → UPLOADED → COMMITTED → CLEANED (terminal, NOT a resume candidate)
	driveForward := func(id string, shaPrefix rune, size int64) {
		_ = s.MarkReady(ctx, id, strings.Repeat(string(shaPrefix), 64), size)
		_ = s.MarkUploadPending(ctx, id, "up_"+id)
		_ = s.MarkUploading(ctx, id, 0)
		_ = s.MarkUploaded(ctx, id)
		_ = s.MarkCommitted(ctx, id)
		_ = s.MarkCleaned(ctx, id)
	}
	driveForward(a4.SpoolID, '4', 13)
	// A5 → REJECTED (terminal, NOT a resume candidate)
	_ = s.MarkReady(ctx, a5.SpoolID, strings.Repeat("5", 64), 14)
	if err := s.MarkRejected(ctx, a5.SpoolID, "E_TEST", "fixture"); err != nil {
		t.Fatalf("MarkRejected → %v", err)
	}

	got, err := s.ListResumeCandidates(ctx)
	if err != nil {
		t.Fatalf("ListResumeCandidates → %v", err)
	}
	ids := make([]string, len(got))
	for i, e := range got {
		ids[i] = e.SpoolID
	}
	sort.Strings(ids)
	want := []string{a1.SpoolID, a2.SpoolID, a3.SpoolID}
	sort.Strings(want)
	if len(ids) != len(want) {
		t.Fatalf("ListResumeCandidates: got %d, want %d (%v)", len(ids), len(want), ids)
	}
	for i := range ids {
		if ids[i] != want[i] {
			t.Errorf("ListResumeCandidates[%d] = %s; want %s", i, ids[i], want[i])
		}
	}
}

// Drive a row all the way through the happy path:
// RENDERING → OUTPUT_READY → UPLOAD_PENDING → UPLOADING → UPLOADED →
// COMMITTED → CLEANED. Verifies every stamping column and the final
// audit-only shape.
func TestLifecycle_HappyPath_StampsAndProgress(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "happy")

	sha := strings.Repeat("0", 64)
	if err := s.MarkReady(ctx, e.SpoolID, sha, 4242); err != nil {
		t.Fatalf("MarkReady → %v", err)
	}
	got, err := s.Get(ctx, e.SpoolID)
	if err != nil {
		t.Fatalf("Get after MarkReady → %v", err)
	}
	if got.Status != StatusOutputReady {
		t.Errorf("after MarkReady: status = %q; want OUTPUT_READY", got.Status)
	}
	if got.SHA256 != sha {
		t.Errorf("after MarkReady: sha256 = %q; want %q", got.SHA256, sha)
	}
	if got.SizeBytes != 4242 {
		t.Errorf("after MarkReady: size = %d; want 4242", got.SizeBytes)
	}
	if got.UpdatedAt.Equal(e.UpdatedAt) {
		t.Errorf("after MarkReady: updated_at did not advance")
	}

	if err := s.MarkUploadPending(ctx, e.SpoolID, "upid_xyz"); err != nil {
		t.Fatalf("MarkUploadPending → %v", err)
	}
	got, _ = s.Get(ctx, e.SpoolID)
	if got.Status != StatusUploadPending || got.UploadID != "upid_xyz" {
		t.Errorf("after MarkUploadPending: status=%q upload_id=%q", got.Status, got.UploadID)
	}

	if err := s.MarkUploading(ctx, e.SpoolID, 0); err != nil {
		t.Fatalf("MarkUploading → %v", err)
	}
	got, _ = s.Get(ctx, e.SpoolID)
	if got.Status != StatusUploading {
		t.Errorf("after MarkUploading: status = %q", got.Status)
	}

	// RecordProgress over multiple batches.
	for _, n := range []int64{100, 500, 1500, 4242} {
		if err := s.RecordProgress(ctx, e.SpoolID, n); err != nil {
			t.Fatalf("RecordProgress(%d) → %v", n, err)
		}
	}
	got, _ = s.Get(ctx, e.SpoolID)
	if got.UploadedBytes != 4242 {
		t.Errorf("after RecordProgress: uploaded_bytes = %d; want 4242", got.UploadedBytes)
	}

	if err := s.MarkUploaded(ctx, e.SpoolID); err != nil {
		t.Fatalf("MarkUploaded → %v", err)
	}
	if err := s.MarkCommitted(ctx, e.SpoolID); err != nil {
		t.Fatalf("MarkCommitted → %v", err)
	}
	got, _ = s.Get(ctx, e.SpoolID)
	if got.Status != StatusCommitted {
		t.Errorf("after MarkCommitted: status = %q", got.Status)
	}
	// LocalPath survives until MarkCleaned (post-commit audit window).
	if got.LocalPath == "" {
		t.Errorf("after MarkCommitted: LocalPath cleared prematurely")
	}

	if err := s.MarkCleaned(ctx, e.SpoolID); err != nil {
		t.Fatalf("MarkCleaned → %v", err)
	}
	got, _ = s.Get(ctx, e.SpoolID)
	if got.Status != StatusCleaned {
		t.Errorf("after MarkCleaned: status = %q", got.Status)
	}
	if got.LocalPath != "" {
		t.Errorf("after MarkCleaned: LocalPath must be empty (audit-only)")
	}
}

// Drive every transition out of every valid from-state with a CAS
// conflict to verify the from-state guards.
func TestLifecycle_CASConflicts_OutOfFromState(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "cas")

	// Each call below must fail with ErrCASConflict because the
	// expected from-state does not match.
	type tc struct {
		name string
		fn   func() error
	}
	cases := []tc{
		{"MarkUploadPending on RENDERING", func() error { return s.MarkUploadPending(ctx, e.SpoolID, "x") }},
		{"MarkUploading on RENDERING", func() error { return s.MarkUploading(ctx, e.SpoolID, 0) }},
		{"RecordProgress on RENDERING", func() error { return s.RecordProgress(ctx, e.SpoolID, 1) }},
		{"MarkUploaded on RENDERING", func() error { return s.MarkUploaded(ctx, e.SpoolID) }},
		{"MarkCommitted on RENDERING", func() error { return s.MarkCommitted(ctx, e.SpoolID) }},
		{"MarkCleaned on RENDERING", func() error { return s.MarkCleaned(ctx, e.SpoolID) }},
	}
	for _, c := range cases {
		err := c.fn()
		if err == nil || !strings.Contains(err.Error(), "lifecycle CAS conflict") {
			t.Errorf("%s: expected ErrCASConflict, got %v", c.name, err)
		}
	}

	// 1) MarkReady → 2) MarkReady again should fail (already moved off RENDERING).
	if err := s.MarkReady(ctx, e.SpoolID, strings.Repeat("f", 64), 64); err != nil {
		t.Fatalf("first MarkReady → %v", err)
	}
	if err := s.MarkReady(ctx, e.SpoolID, strings.Repeat("f", 64), 64); err == nil {
		t.Fatalf("second MarkReady: expected CAS conflict")
	}
}

// MarkReject must NOT clobber a row that is already terminal.
func TestLifecycle_MarkRejected_DoesNotClobberTerminal(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()

	// a1 will be moved to COMMITTED+CLEANED → immutable
	a1 := mustInsertBasic(t, s, "imm")
	drive := func(id string) {
		_ = s.MarkReady(ctx, id, strings.Repeat("0", 64), 1)
		_ = s.MarkUploadPending(ctx, id, "upid")
		_ = s.MarkUploading(ctx, id, 0)
		_ = s.MarkUploaded(ctx, id)
		_ = s.MarkCommitted(ctx, id)
		_ = s.MarkCleaned(ctx, id)
	}
	drive(a1.SpoolID)
	if err := s.MarkRejected(ctx, a1.SpoolID, "E_LATE", "ignored"); err == nil {
		t.Fatalf("MarkRejected on CLEANED: expected CAS conflict")
	}
	got, _ := s.Get(ctx, a1.SpoolID)
	if got.Status != StatusCleaned {
		t.Errorf("MarkRejected must not change terminal state, got %q", got.Status)
	}

	// a2 will be REJECTED → REJECTED must be idempotent under CAS (no
	// double-row mutation; the second call MUST conflict).
	a2 := mustInsertBasic(t, s, "rej")
	_ = s.MarkReady(ctx, a2.SpoolID, strings.Repeat("0", 64), 1)
	if err := s.MarkRejected(ctx, a2.SpoolID, "E_TEST", "first"); err != nil {
		t.Fatalf("MarkRejected → %v", err)
	}
	if err := s.MarkRejected(ctx, a2.SpoolID, "E_TEST", "second"); err == nil {
		t.Fatalf("MarkRejected again: expected CAS conflict")
	}
	got, _ = s.Get(ctx, a2.SpoolID)
	if got.LastError == "" || got.LastError == "-" {
		t.Errorf("after MarkRejected: last_error empty? got %q", got.LastError)
	}

	// a3 in OUTPUT_READY → REJECTED → CLEANED (full reject lifecycle).
	a3 := mustInsertBasic(t, s, "rejclean")
	_ = s.MarkReady(ctx, a3.SpoolID, strings.Repeat("0", 64), 1)
	if err := s.MarkRejected(ctx, a3.SpoolID, "E_BROKEN", "out.mp4 corrupt"); err != nil {
		t.Fatalf("MarkRejected → %v", err)
	}
	if err := s.MarkCleaned(ctx, a3.SpoolID); err != nil {
		t.Fatalf("MarkCleaned on REJECTED → %v", err)
	}
	got, _ = s.Get(ctx, a3.SpoolID)
	if got.Status != StatusCleaned {
		t.Errorf("after MarkCleaned on REJECTED: status = %q", got.Status)
	}
}

func TestLifecycle_MarkReady_BadShaLength_Errors(t *testing.T) {
	s := newInMemoryTestStore(t)
	e := mustInsertBasic(t, s, "sha")
	err := s.MarkReady(context.Background(), e.SpoolID, "deadbeef", 10)
	if err == nil || !strings.Contains(err.Error(), "64 hex") {
		t.Fatalf("MarkReady(short sha) → %v; want 64-hex error", err)
	}
}

func TestLifecycle_RecordProgress_NonUploadingDoesNotMatch(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "prog")

	// Row is in RENDERING → RecordProgress must NOT match.
	if err := s.RecordProgress(ctx, e.SpoolID, 100); err == nil {
		t.Fatalf("RecordProgress on RENDERING: expected CAS conflict")
	}
}

func TestLifecycle_TimestampMonotonic(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "monotonic")
	first := e.UpdatedAt
	// Sleep at least 1µs so any subsequent Format is strictly greater.
	time.Sleep(2 * time.Microsecond)
	if err := s.MarkReady(ctx, e.SpoolID, strings.Repeat("9", 64), 1); err != nil {
		t.Fatalf("MarkReady → %v", err)
	}
	got, _ := s.Get(ctx, e.SpoolID)
	if !got.UpdatedAt.After(first) {
		t.Errorf("UpdatedAt did not advance: first=%v second=%v", first, got.UpdatedAt)
	}
}

func TestDelete_Operational(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "del")
	if err := s.Delete(ctx, e.SpoolID); err != nil {
		t.Fatalf("Delete → %v", err)
	}
	if _, err := s.Get(ctx, e.SpoolID); err != ErrNotFound {
		t.Fatalf("after Delete Get → %v; want ErrNotFound", err)
	}
}
