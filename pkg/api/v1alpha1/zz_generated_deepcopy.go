// Hand-written DeepCopy methods. The filename mirrors what
// controller-gen would produce so a future codegen pass can
// overwrite it without renaming.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (in *DefaultAccess) DeepCopyInto(out *DefaultAccess) { *out = *in }
func (in *DefaultAccess) DeepCopy() *DefaultAccess {
	if in == nil {
		return nil
	}
	out := new(DefaultAccess)
	in.DeepCopyInto(out)
	return out
}

func (in *BucketyRef) DeepCopyInto(out *BucketyRef) { *out = *in }
func (in *BucketyRef) DeepCopy() *BucketyRef {
	if in == nil {
		return nil
	}
	out := new(BucketyRef)
	in.DeepCopyInto(out)
	return out
}

func (in *BucketySpec) DeepCopyInto(out *BucketySpec) {
	*out = *in
	if in.Parameters != nil {
		out.Parameters = make(map[string]string, len(in.Parameters))
		for k, v := range in.Parameters {
			out.Parameters[k] = v
		}
	}
	if in.DefaultAccess != nil {
		out.DefaultAccess = in.DefaultAccess.DeepCopy()
	}
}
func (in *BucketySpec) DeepCopy() *BucketySpec {
	if in == nil {
		return nil
	}
	out := new(BucketySpec)
	in.DeepCopyInto(out)
	return out
}

func (in *BucketyStatus) DeepCopyInto(out *BucketyStatus) {
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}
func (in *BucketyStatus) DeepCopy() *BucketyStatus {
	if in == nil {
		return nil
	}
	out := new(BucketyStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *Buckety) DeepCopyInto(out *Buckety) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}
func (in *Buckety) DeepCopy() *Buckety {
	if in == nil {
		return nil
	}
	out := new(Buckety)
	in.DeepCopyInto(out)
	return out
}
func (in *Buckety) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *BucketyList) DeepCopyInto(out *BucketyList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Buckety, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
func (in *BucketyList) DeepCopy() *BucketyList {
	if in == nil {
		return nil
	}
	out := new(BucketyList)
	in.DeepCopyInto(out)
	return out
}
func (in *BucketyList) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *BucketyAccessSpec) DeepCopyInto(out *BucketyAccessSpec) {
	*out = *in
	out.BucketyRef = in.BucketyRef
	if in.Parameters != nil {
		out.Parameters = make(map[string]string, len(in.Parameters))
		for k, v := range in.Parameters {
			out.Parameters[k] = v
		}
	}
}
func (in *BucketyAccessSpec) DeepCopy() *BucketyAccessSpec {
	if in == nil {
		return nil
	}
	out := new(BucketyAccessSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *BucketyAccessStatus) DeepCopyInto(out *BucketyAccessStatus) {
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}
func (in *BucketyAccessStatus) DeepCopy() *BucketyAccessStatus {
	if in == nil {
		return nil
	}
	out := new(BucketyAccessStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *BucketyAccess) DeepCopyInto(out *BucketyAccess) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}
func (in *BucketyAccess) DeepCopy() *BucketyAccess {
	if in == nil {
		return nil
	}
	out := new(BucketyAccess)
	in.DeepCopyInto(out)
	return out
}
func (in *BucketyAccess) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *BucketyAccessList) DeepCopyInto(out *BucketyAccessList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]BucketyAccess, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
func (in *BucketyAccessList) DeepCopy() *BucketyAccessList {
	if in == nil {
		return nil
	}
	out := new(BucketyAccessList)
	in.DeepCopyInto(out)
	return out
}
func (in *BucketyAccessList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
