package slo

import "testing"

func TestSLOCatalog(t *testing.T) {
	if len(Catalog) != 6 { t.Fatalf("catalog=%d", len(Catalog)) }
	if !Meets(Catalog[0], .995) || Meets(Catalog[0], .9) { t.Fatal("SLO comparison incorrect") }
}
