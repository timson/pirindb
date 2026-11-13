package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (in *PirinDBStorageSpec) DeepCopyInto(out *PirinDBStorageSpec) {
	*out = *in
	if in.StorageClassName != nil {
		value := *in.StorageClassName
		out.StorageClassName = &value
	}
}

func (in *PirinDBStorageSpec) DeepCopy() *PirinDBStorageSpec {
	if in == nil {
		return nil
	}
	out := new(PirinDBStorageSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *PirinDBClusterSpec) DeepCopyInto(out *PirinDBClusterSpec) {
	*out = *in
	if in.Replicas != nil {
		value := *in.Replicas
		out.Replicas = &value
	}
	in.Storage.DeepCopyInto(&out.Storage)
	if in.AutoRebalanceOnScaleUp != nil {
		value := *in.AutoRebalanceOnScaleUp
		out.AutoRebalanceOnScaleUp = &value
	}
	if in.AutoRebalanceOnScaleDown != nil {
		value := *in.AutoRebalanceOnScaleDown
		out.AutoRebalanceOnScaleDown = &value
	}
	if in.AdminSecretRef != nil {
		out.AdminSecretRef = in.AdminSecretRef.DeepCopy()
	}
	out.Resources = *in.Resources.DeepCopy()
}

func (in *PirinDBClusterSpec) DeepCopy() *PirinDBClusterSpec {
	if in == nil {
		return nil
	}
	out := new(PirinDBClusterSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *PirinDBClusterRuntimeCondition) DeepCopyInto(out *PirinDBClusterRuntimeCondition) {
	*out = *in
}

func (in *PirinDBClusterRuntimeCondition) DeepCopy() *PirinDBClusterRuntimeCondition {
	if in == nil {
		return nil
	}
	out := new(PirinDBClusterRuntimeCondition)
	in.DeepCopyInto(out)
	return out
}

func (in *PirinDBClusterRuntimeNodeStatus) DeepCopyInto(out *PirinDBClusterRuntimeNodeStatus) {
	*out = *in
}

func (in *PirinDBClusterRuntimeNodeStatus) DeepCopy() *PirinDBClusterRuntimeNodeStatus {
	if in == nil {
		return nil
	}
	out := new(PirinDBClusterRuntimeNodeStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *PirinDBClusterOperationStatus) DeepCopyInto(out *PirinDBClusterOperationStatus) {
	*out = *in
	if in.StartedAt != nil {
		value := in.StartedAt.DeepCopy()
		out.StartedAt = value
	}
	if in.FinishedAt != nil {
		value := in.FinishedAt.DeepCopy()
		out.FinishedAt = value
	}
}

func (in *PirinDBClusterOperationStatus) DeepCopy() *PirinDBClusterOperationStatus {
	if in == nil {
		return nil
	}
	out := new(PirinDBClusterOperationStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *PirinDBClusterStatus) DeepCopyInto(out *PirinDBClusterStatus) {
	*out = *in
	if in.RuntimeConditions != nil {
		out.RuntimeConditions = make([]PirinDBClusterRuntimeCondition, len(in.RuntimeConditions))
		copy(out.RuntimeConditions, in.RuntimeConditions)
	}
	if in.RuntimeNodes != nil {
		out.RuntimeNodes = make([]PirinDBClusterRuntimeNodeStatus, len(in.RuntimeNodes))
		copy(out.RuntimeNodes, in.RuntimeNodes)
	}
	if in.ActiveOperation != nil {
		out.ActiveOperation = in.ActiveOperation.DeepCopy()
	}
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		copy(out.Conditions, in.Conditions)
	}
}

func (in *PirinDBClusterStatus) DeepCopy() *PirinDBClusterStatus {
	if in == nil {
		return nil
	}
	out := new(PirinDBClusterStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *PirinDBCluster) DeepCopyInto(out *PirinDBCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *PirinDBCluster) DeepCopy() *PirinDBCluster {
	if in == nil {
		return nil
	}
	out := new(PirinDBCluster)
	in.DeepCopyInto(out)
	return out
}

func (in *PirinDBCluster) DeepCopyObject() runtime.Object {
	if copy := in.DeepCopy(); copy != nil {
		return copy
	}
	return nil
}

func (in *PirinDBClusterList) DeepCopyInto(out *PirinDBClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]PirinDBCluster, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *PirinDBClusterList) DeepCopy() *PirinDBClusterList {
	if in == nil {
		return nil
	}
	out := new(PirinDBClusterList)
	in.DeepCopyInto(out)
	return out
}

func (in *PirinDBClusterList) DeepCopyObject() runtime.Object {
	if copy := in.DeepCopy(); copy != nil {
		return copy
	}
	return nil
}
