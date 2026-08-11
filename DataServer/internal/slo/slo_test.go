package slo

import "testing"

func TestSLOCatalog(t *testing.T) {
	if len(catalog) != 6 {
		t.Fatalf("catalog=%d", len(catalog))
	}
	if !Meets(catalog[0], .995) || Meets(catalog[0], .9) {
		t.Fatal("SLO comparison incorrect")
	}
}
