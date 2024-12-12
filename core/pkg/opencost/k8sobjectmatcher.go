package opencost

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	kfilter "github.com/opencost/opencost/core/pkg/filter/k8sobject"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/filter/transform"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// K8sObjectMatcher is a matcher implementation for Kubernetes runtime.Object
// instances, compiled using the matcher.MatchCompiler.
type K8sObjectMatcher matcher.Matcher[runtime.Object]

// NewK8sObjectMatchCompiler creates a new instance of a
// matcher.MatchCompiler[runtime.Object] which can be used to compile
// filter.Filter ASTs into matcher.Matcher[runtime.Object] implementations.
//
// If the label config is nil, the compiler will fail to compile alias filters
// if any are present in the AST.
func NewK8sObjectMatchCompiler() *matcher.MatchCompiler[runtime.Object] {
	passes := []transform.CompilerPass{}

	return matcher.NewMatchCompiler(
		k8sObjectFieldMap,
		k8sObjectSliceFieldMap,
		k8sObjectMapFieldMap,
		passes...,
	)
}

func objectMetaFromObject(o runtime.Object) (metav1.ObjectMeta, error) {
	switch v := o.(type) {
	case *appsv1.Deployment:
		return v.ObjectMeta, nil
	case *appsv1.StatefulSet:
		return v.ObjectMeta, nil
	case *appsv1.DaemonSet:
		return v.ObjectMeta, nil
	case *corev1.Pod:
		return v.ObjectMeta, nil
	case *batchv1.CronJob:
		return v.ObjectMeta, nil
	}

	return metav1.ObjectMeta{}, fmt.Errorf("currently-unsupported runtime.Object type for filtering: %T", o)
}

// Maps fields from an allocation to a string value based on an identifier
func k8sObjectFieldMap(o runtime.Object, identifier ast.Identifier) (string, error) {
	if identifier.Field == nil {
		return "", fmt.Errorf("cannot map field from identifier with nil field")
	}

	m, err := objectMetaFromObject(o)
	if err != nil {
		return "", fmt.Errorf("retrieving object meta: %w", err)
	}
	var controllerKind string
	var controllerName string
	var pod string

	switch v := o.(type) {
	case *appsv1.Deployment:
		controllerKind = "deployment"
		controllerName = v.Name
	case *appsv1.StatefulSet:
		controllerKind = "statefulset"
		controllerName = v.Name
	case *appsv1.DaemonSet:
		controllerKind = "daemonset"
		controllerName = v.Name
	case *corev1.Pod:
		pod = v.Name
		if len(v.OwnerReferences) == 0 {
			controllerKind = "pod"
			controllerName = v.Name
		}
	case *batchv1.CronJob:
		controllerKind = "cronjob"
		controllerName = v.Name
	default:
		return "", fmt.Errorf("currently-unsupported runtime.Object type for filtering: %T", o)
	}

	// For now, we will just do our best to implement Allocation fields because
	// most k8s-based queries are on Allocation data. The other we will
	// eventually want to support is Asset, but I'm not sure that I have time
	// for that right now.
	field := kfilter.K8sObjectField(identifier.Field.Name)
	switch field {
	case kfilter.FieldNamespace:
		return m.Namespace, nil
	case kfilter.FieldControllerName:
		return controllerName, nil
	case kfilter.FieldControllerKind:
		return controllerKind, nil
	case kfilter.FieldPod:
		return pod, nil
	case kfilter.FieldLabel:
		if m.Labels != nil {
			return m.Labels[identifier.Key], nil
		}
		return "", nil
	case kfilter.FieldAnnotation:
		if m.Annotations != nil {
			return m.Annotations[identifier.Key], nil
		}
		return "", nil
	}

	return "", fmt.Errorf("Failed to find string identifier on K8sObject: %s (consider adding support if this is an expected field)", identifier.Field.Name)
}

// Maps slice fields from an allocation to a []string value based on an identifier
func k8sObjectSliceFieldMap(o runtime.Object, identifier ast.Identifier) ([]string, error) {
	return nil, fmt.Errorf("K8sObject filters current have no supported []string identifiers")
}

// Maps map fields from an allocation to a map[string]string value based on an identifier
func k8sObjectMapFieldMap(o runtime.Object, identifier ast.Identifier) (map[string]string, error) {
	m, err := objectMetaFromObject(o)
	if err != nil {
		return nil, fmt.Errorf("retrieving object meta: %w", err)
	}
	switch kfilter.K8sObjectField(identifier.Field.Name) {
	case kfilter.FieldLabel:
		return m.Labels, nil
	case kfilter.FieldAnnotation:
		return m.Annotations, nil
	}
	return nil, fmt.Errorf("Failed to find map[string]string identifier on K8sObject: %s", identifier.Field.Name)
}
