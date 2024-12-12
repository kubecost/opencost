package opencost

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	k8sobject "github.com/opencost/opencost/core/pkg/filter/k8sobject"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestK8sObjectMatcher(t *testing.T) {
	cases := []struct {
		filter string
		o      runtime.Object

		expected bool
	}{
		{
			filter: `namespace:"kubecost"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "kubecost",
				},
			},
			expected: true,
		},
		{
			filter: `namespace:"kubecost"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "kube-system",
				},
			},
			expected: false,
		},
		{
			filter: `pod:"foo"`,
			o: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			},
			expected: true,
		},
		{
			filter: `pod:"foo"`,
			o: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "bar"},
			},
			expected: false,
		},
		{
			filter: `pod:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			},
			expected: false,
		},
		{
			filter:   `controllerKind:"deployment"`,
			o:        &appsv1.Deployment{},
			expected: true,
		},
		{
			filter: `controllerName:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			},
			expected: true,
		},
		{
			filter:   `controllerKind:"statefulset"`,
			o:        &appsv1.StatefulSet{},
			expected: true,
		},
		{
			filter: `controllerName:"foo"`,
			o: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			},
			expected: true,
		},
		{
			filter:   `controllerKind:"daemonset"`,
			o:        &appsv1.DaemonSet{},
			expected: true,
		},
		{
			filter: `controllerName:"foo"`,
			o: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			},
			expected: true,
		},
		{
			filter:   `controllerKind:"cronjob"`,
			o:        &batchv1.CronJob{},
			expected: true,
		},
		{
			filter: `controllerName:"foo"`,
			o: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			},
			expected: true,
		},
		{
			filter:   `controllerKind:"pod"`,
			o:        &corev1.Pod{},
			expected: true,
		},
		{
			filter: `controllerKind:"pod"`,
			o: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{}, // Having an owner reference makes this Pod "controlled"
					},
				},
			},
			expected: false,
		},
		{
			filter: `label[app]:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "foo"},
				},
			},
			expected: true,
		},
		{
			filter: `label[app]:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "bar"},
				},
			},
			expected: false,
		},
		{
			filter: `label[app]:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			expected: false,
		},
		{
			filter: `label[app]:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Labels: nil,
				},
			},
			expected: false,
		},
		{
			filter: `annotation[app]:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"app": "foo"},
				},
			},
			expected: true,
		},
		{
			filter: `annotation[app]:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"app": "bar"},
				},
			},
			expected: false,
		},
		{
			filter: `annotation[app]:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			},
			expected: false,
		},
		{
			filter: `annotation[app]:"foo"`,
			o: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: nil,
				},
			},
			expected: false,
		},
	}

	for _, c := range cases {
		t.Run(c.filter, func(t *testing.T) {
			parser := k8sobject.NewK8sObjectFilterParser()
			parsed, err := parser.Parse(c.filter)
			if err != nil {
				t.Fatalf("parsing '%s': %s", c.filter, err)
			}
			t.Logf("Parsed: %s", ast.ToPreOrderString(parsed))

			compiler := NewK8sObjectMatchCompiler()
			matcher, err := compiler.Compile(parsed)
			if err != nil {
				t.Fatalf("compiling: %s", err)
			}
			t.Logf("Compiled: %s", matcher.String())

			result := matcher.Matches(c.o)

			if result != c.expected {
				t.Errorf("Expected %t, got %t", c.expected, result)
			}
		})
	}
}
