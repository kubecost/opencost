package k8sobject

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

// K8sObjectField is an enum that represents K8sObject-specific fields that can
// be filtered on.
type K8sObjectField string

const (
	FieldNamespace      K8sObjectField = K8sObjectField(fieldstrings.FieldNamespace)
	FieldControllerKind K8sObjectField = K8sObjectField(fieldstrings.FieldControllerKind)
	FieldControllerName K8sObjectField = K8sObjectField(fieldstrings.FieldControllerName)
	FieldPod            K8sObjectField = K8sObjectField(fieldstrings.FieldPod)
	FieldLabel          K8sObjectField = K8sObjectField(fieldstrings.FieldLabel)
	FieldAnnotation     K8sObjectField = K8sObjectField(fieldstrings.FieldAnnotation)
)
