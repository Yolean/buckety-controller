package v1alpha1

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The deepcopy methods are hand-written (zz_generated_deepcopy.go
// mirrors the controller-gen filename but nothing generates it).
// The hazard is a new reference-typed field that *out = *in copies
// as an alias. These tests populate every field, copy, then mutate
// the original's reference values and require the copy unchanged.

func populatedBuckety() *Buckety {
	return &Buckety{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders",
			Namespace: "tenant1",
			Labels:    map[string]string{"yolean.se/generation": "003"},
		},
		Spec: BucketySpec{
			Backend:         "kafka",
			Name:            "${namespace}.${name}",
			Parameters:      map[string]string{"partitions": "3"},
			RetentionPolicy: RetentionDelete,
			DefaultAccess: &DefaultAccess{
				Role:                  RoleReadWrite,
				CredentialsSecretName: "orders-topic",
			},
		},
		Status: BucketyStatus{
			ObservedGeneration:  2,
			Backend:             "kafka",
			Driver:              "kadm",
			DriverMajor:         0,
			DriverBuildVersion:  "0.1.0",
			BackendResourceName: "tenant1.orders",
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue,
				Reason: "EnsuredOnBackend", Message: "",
			}},
		},
	}
}

func populatedAccess() *BucketyAccess {
	return &BucketyAccess{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-reader", Namespace: "tenant1"},
		Spec: BucketyAccessSpec{
			BucketyRef:            BucketyRef{Name: "orders"},
			CredentialsSecretName: "orders-reader",
			Role:                  RoleReader,
			Parameters:            map[string]string{"consumerGroupPrefix": "x-"},
		},
		Status: BucketyAccessStatus{
			ObservedGeneration: 1,
			Principal:          "kadm-root",
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue,
				Reason: "SecretMinted",
			}},
		},
	}
}

func TestBucketyDeepCopyDoesNotAlias(t *testing.T) {
	orig := populatedBuckety()
	cp := orig.DeepCopy()
	if !reflect.DeepEqual(orig, cp) {
		t.Fatal("copy differs from original before mutation")
	}
	orig.Labels["yolean.se/generation"] = "mutated"
	orig.Spec.Parameters["partitions"] = "mutated"
	orig.Spec.DefaultAccess.CredentialsSecretName = "mutated"
	orig.Status.Conditions[0].Reason = "mutated"
	if reflect.DeepEqual(orig, cp) {
		t.Fatal("mutation propagated nowhere; test fixture is broken")
	}
	fresh := populatedBuckety()
	if !reflect.DeepEqual(fresh, cp) {
		t.Fatalf("copy changed when original was mutated (aliased reference field):\n copy: %+v\nfresh: %+v", cp, fresh)
	}
}

func TestBucketyAccessDeepCopyDoesNotAlias(t *testing.T) {
	orig := populatedAccess()
	cp := orig.DeepCopy()
	if !reflect.DeepEqual(orig, cp) {
		t.Fatal("copy differs from original before mutation")
	}
	orig.Spec.Parameters["consumerGroupPrefix"] = "mutated"
	orig.Status.Conditions[0].Reason = "mutated"
	fresh := populatedAccess()
	if !reflect.DeepEqual(fresh, cp) {
		t.Fatalf("copy changed when original was mutated (aliased reference field):\n copy: %+v\nfresh: %+v", cp, fresh)
	}
}

func TestListDeepCopy(t *testing.T) {
	bl := &BucketyList{Items: []Buckety{*populatedBuckety()}}
	blc := bl.DeepCopy()
	bl.Items[0].Spec.Parameters["partitions"] = "mutated"
	if blc.Items[0].Spec.Parameters["partitions"] == "mutated" {
		t.Fatal("BucketyList copy aliases item parameters")
	}
	al := &BucketyAccessList{Items: []BucketyAccess{*populatedAccess()}}
	alc := al.DeepCopy()
	al.Items[0].Spec.Parameters["consumerGroupPrefix"] = "mutated"
	if alc.Items[0].Spec.Parameters["consumerGroupPrefix"] == "mutated" {
		t.Fatal("BucketyAccessList copy aliases item parameters")
	}
}
