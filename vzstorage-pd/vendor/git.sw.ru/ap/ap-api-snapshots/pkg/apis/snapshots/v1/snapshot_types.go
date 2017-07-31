/* Copyright (c) 2017 Parallels IP Holdings GmbH */

package v1

import (
	"log"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"git.sw.ru/ap/ap-api-snapshots/pkg/apis/snapshots"
)

// +genclient=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Snapshot
// +k8s:openapi-gen=true
// +resource:path=snapshots,strategy=SnapshotStrategy
type Snapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotSpec   `json:"spec,omitempty"`
	Status SnapshotStatus `json:"status,omitempty"`
}

// SnapshotSpec defines the desired state of Snapshot
type SnapshotSpec struct {
	// Optional: Extra command options if any.
	// +optional
	Options map[string]string `json:"options,omitempty"`
	// +optional
	SnapshottedVolumeClaim string `json:"persistentVolumeClaim,omitempty"`
}

const (
	SnapshotPending     string = "Pending"
	SnapshotScheduled   string = "Scheduled"
	SnapshotAvailable   string = "Available"
	SnapshotTerminating string = "Terminating"
	SnapshotFailed      string = "Failed"
)

// SnapshotStatus defines the observed state of Snapshot
type SnapshotStatus struct {
	// Phase represents the current phase of Snapshot
	// +optional
	Phase string `json:"phase,,omitempty"`
	// A human readable message indicating details about why the pod is in this condition.
	// +optional
	Message string `json:"message,omitempty"`
	// A pod name which creates snapshot
	// +optional
	PodName string `json:"pod,omitempty"`
}

func (SnapshotStrategy) CheckGracefulDelete(ctx request.Context, o runtime.Object, options *metav1.DeleteOptions) bool {
	obj := o.(*snapshots.Snapshot)
	if options == nil {
		return false
	}
	period := int64(0)
	if options.GracePeriodSeconds != nil {
		period = *options.GracePeriodSeconds
	} else {
		period = int64(120)
	}
	if obj.Status.Phase == SnapshotFailed ||
		obj.Status.Phase == SnapshotPending {
		period = 0
	}
	options.GracePeriodSeconds = &period
	log.Printf("Delete %s: period=%d\n", obj.Name, period)
	return true
}

// Validate checks that an instance of Snapshot is well formed
func (SnapshotStrategy) Validate(ctx request.Context, obj runtime.Object) field.ErrorList {
	o := obj.(*snapshots.Snapshot)
	log.Printf("Validating fields for Snapshot %s\n", o.Name)
	errors := field.ErrorList{}
	// perform validation here and add to errors using field.Invalid
	return errors
}

// DefaultingFunction sets default Snapshot field values
func (SnapshotSchemeFns) DefaultingFunction(o interface{}) {
	obj := o.(*Snapshot)
	// set default field values here
	if obj.Status.Phase == "" {
		obj.Status.Phase = SnapshotPending
	}
	log.Printf("Defaulting fields for Snapshot %s\n", obj.Name)
}
