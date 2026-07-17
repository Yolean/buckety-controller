package buckety

import (
	"testing"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

func TestMajorOf(t *testing.T) {
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"0.1.0", 0, false},
		{"1.0.0", 1, false},
		{"12.3.4", 12, false},
		{"7", 7, false},
		{"", 0, true},
		{"v1.0.0", 0, true},
		{"one.two", 0, true},
	}
	for _, c := range cases {
		got, err := majorOf(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("majorOf(%q): expected error, got %d", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("majorOf(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("majorOf(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// The adoption gate's decision table (SPEC "Adoption").
func TestDecideProvenance(t *testing.T) {
	cases := []struct {
		name   string
		insp   registry.Inspection
		policy bucketyv1.AdoptionPolicy
		want   bucketyv1.Provenance
	}{
		{"fresh name", registry.Inspection{}, "", bucketyv1.ProvenanceCreated},
		{"fresh name ignores Adopt", registry.Inspection{}, bucketyv1.AdoptionAdopt, bucketyv1.ProvenanceCreated},
		{"exists empty adopts by default", registry.Inspection{Exists: true, Empty: true}, "", bucketyv1.ProvenanceAdopted},
		{"exists empty under AdoptEmpty", registry.Inspection{Exists: true, Empty: true}, bucketyv1.AdoptionAdoptEmpty, bucketyv1.ProvenanceAdopted},
		{"content refused by default", registry.Inspection{Exists: true}, "", ""},
		{"content refused under AdoptEmpty", registry.Inspection{Exists: true}, bucketyv1.AdoptionAdoptEmpty, ""},
		{"content claimed with Adopt", registry.Inspection{Exists: true}, bucketyv1.AdoptionAdopt, bucketyv1.ProvenanceAdopted},
	}
	for _, c := range cases {
		if got := decideProvenance(c.insp, c.policy); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}
