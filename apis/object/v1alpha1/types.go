/*
Copyright 2020 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/fieldpath"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ObjectParameters are the configurable fields of a Object.
type ObjectParameters struct {
	// Raw JSON representation of the kubernetes object to be created.
	// +kubebuilder:validation:EmbeddedResource
	// +kubebuilder:pruning:PreserveUnknownFields
	Manifest runtime.RawExtension `json:"manifest"`
}

// ObjectObservation are the observable fields of a Object.
type ObjectObservation struct {
	// Raw JSON representation of the remote object.
	// +kubebuilder:validation:EmbeddedResource
	// +kubebuilder:pruning:PreserveUnknownFields
	Manifest runtime.RawExtension `json:"manifest,omitempty"`
}

// FromObject is a reference to another object.
type FromObject struct {
	// APIVersion of the object.
	APIVersion string `json:"apiVersion"`
	// Kind of the object.
	Kind string `json:"kind"`

	// Name of the object.
	Name string `json:"name"`
	// Namespace of the object.
	// +optional
	Namespace string `json:"namespace"`

	// FieldPath shows the path of the field whose value will be used.
	// +optional
	FieldPath *string `json:"fieldPath"`
}

// Reference is the referenced resource of an Object.
type Reference struct {
	// Reference to another managed resource
	FromObject `json:"fromObject"`
	// ToFieldPath is the path of the field on the resource whose value will
	// be changed with the result of transforms. Leave empty if you'd like to
	// propagate to the same path as fieldPath.
	// +optional
	ToFieldPath *string `json:"toFieldPath,omitempty"`
}

// A ObjectSpec defines the desired state of a Object.
type ObjectSpec struct {
	xpv1.ResourceSpec `json:",inline"`
	References        []Reference      `json:"references,omitempty"`
	ForProvider       ObjectParameters `json:"forProvider"`
}

// A ObjectStatus represents the observed state of a Object.
type ObjectStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          ObjectObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// A Object is an provider Kubernetes API type
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:scope=Cluster,categories={crossplane,managed,kubernetes}
type Object struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ObjectSpec   `json:"spec"`
	Status ObjectStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ObjectList contains a list of Object
type ObjectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Object `json:"items"`
}

// ApplyFromFieldPathPatch patches the "to" resource, using a source field
// on the "from" resource.
func (d *Reference) ApplyFromFieldPathPatch(from, to runtime.Object) error {
	if d.FromObject.FieldPath == nil {
		return nil
	}

	// Default to patching the same field on the "to" resource.
	if d.ToFieldPath == nil {
		// d.ToFieldPath = d.FromObject.FieldPath
		return nil
	}

	fromMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(from)
	if err != nil {
		return err
	}

	out, err := fieldpath.Pave(fromMap).GetValue(*d.FromObject.FieldPath)
	if err != nil {
		return err
	}

	return patchFieldValueToObject(*d.ToFieldPath, out, to)
}

// patchFieldValueToObject, given a path, value and "to" object, will
// apply the value to the "to" object at the given path, returning
// any errors as they occur.
func patchFieldValueToObject(path string, value interface{}, to runtime.Object) error {
	if u, ok := to.(interface{ UnstructuredContent() map[string]interface{} }); ok {
		return fieldpath.Pave(u.UnstructuredContent()).SetValue(path, value)
	}

	toMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(to)
	if err != nil {
		return err
	}
	if err := fieldpath.Pave(toMap).SetValue(path, value); err != nil {
		return err
	}
	return runtime.DefaultUnstructuredConverter.FromUnstructured(toMap, to)
}
