package contract

import "testing"

func TestCanonicalVideoStreamSignatureV1Default(t *testing.T) {
	signature := CanonicalVideoStreamSignatureV1Default()
	if err := signature.Validate(); err != nil {
		t.Fatalf("default signature: %v", err)
	}
	if signature.TimeBaseNum != 1 || signature.TimeBaseDen != 90000 {
		t.Fatalf("timebase = %d/%d, want 1/90000", signature.TimeBaseNum, signature.TimeBaseDen)
	}
	if signature.ExtradataSHA256 != CanonicalVideoExtradataSHA256 {
		t.Fatalf("extradata = %q", signature.ExtradataSHA256)
	}
	first, err := signature.SHA256()
	if err != nil || len(first) != 64 {
		t.Fatalf("signature SHA256 = %q, %v", first, err)
	}
	second, err := signature.SHA256()
	if err != nil || first != second {
		t.Fatalf("signature must be stable: %q/%q, %v", first, second, err)
	}

	bad := signature
	bad.ExtradataSHA256 = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("an empty avcC identity must be rejected")
	}
}
