package worker

import (
	"context"
	"testing"
)

func TestArtifactResumeSkipsForegroundOwnedArtifact(t *testing.T) {
	w := &Worker{
		publisherPool: NewPublisherPool(1),
		artifactLocks: NewArtifactLockRegistry(),
	}
	foreground, err := w.acquireResumePublishersForTest([]string{"artifact-1"})
	if err != nil {
		t.Fatal(err)
	}
	defer foreground()

	if release, ok := w.tryAcquireResumePublishers([]string{"artifact-1"}); ok || release != nil {
		t.Fatal("resume acquired an artifact owned by foreground publication")
	}
}

func TestArtifactResumeAcquiresAfterForegroundRelease(t *testing.T) {
	w := &Worker{
		publisherPool: NewPublisherPool(1),
		artifactLocks: NewArtifactLockRegistry(),
	}
	foreground, err := w.acquireResumePublishersForTest([]string{"artifact-1"})
	if err != nil {
		t.Fatal(err)
	}
	foreground()

	resume, ok := w.tryAcquireResumePublishers([]string{"artifact-1"})
	if !ok || resume == nil {
		t.Fatal("resume did not acquire the artifact after foreground release")
	}
	resume()
}

// acquireResumePublishersForTest uses the existing blocking path to model
// foreground ownership without coupling these tests to publication details.
func (w *Worker) acquireResumePublishersForTest(keys []string) (func(), error) {
	return w.acquireResumePublishers(context.Background(), keys)
}
