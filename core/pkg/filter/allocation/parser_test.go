package allocation

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hashicorp/go-multierror"
	"github.com/opencost/opencost/core/pkg/filter/ast"
)

var parser ast.FilterParser = NewAllocationFilterParser()

func TestParse(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name: "Empty",
			input: `              
			
			`,
		},
		{
			name:  "Single",
			input: `namespace: "kubecost"`,
		},
		{
			name:  "Single Group",
			input: `(namespace: "kubecost")`,
		},
		{
			name:  "Single Double Group",
			input: `((namespace: "kubecost"))`,
		},
		{
			name:  "And 2x Expression",
			input: `(namespace: "kubecost" + services~:"foo")`,
		},
		{
			name:  "And 4x Expression",
			input: `(namespace: "kubecost" + services~:"foo" + cluster:"cluster-one" + controllerKind:"deployment")`,
		},
		{
			name:  "Nested And Groups",
			input: `namespace: "kubecost" + services~:"foo" + (cluster:"cluster-one" + controllerKind:"deployment")`,
		},
		{
			name:  "Nested Or Groups",
			input: `namespace: "kubecost" | services~:"foo" | (cluster:"cluster-one" | controllerKind:"deployment")`,
		},
		{
			name:  "Nested AndOr Groups",
			input: `namespace: "kubecost" + services~:"foo" + (cluster:"cluster-one" | controllerKind:"deployment")`,
		},
		{
			name:  "Nested OrAnd Groups",
			input: `namespace: "kubecost" | services~:"foo" | (cluster:"cluster-one" + controllerKind:"deployment")`,
		},
		{
			name:  "Nested OrAndOr Groups",
			input: `namespace: "kubecost" | services~:"foo" | (cluster:"cluster-one" + controllerKind:"deployment") | namespace:"bar","test"`,
		},
		{
			name:  "Non-uniform Whitespace",
			input: `node:"node a b c" , "node 12 3"` + string('\n') + "+" + string('\n') + string('\r') + `namespace : "kubecost"`,
		},
		{
			name:  "Group Or Comparison",
			input: `(namespace:"kubecost" | cluster<~:"cluster-") + services~:"foo"`,
		},
		{
			name:  "Group Or Group",
			input: `(label~:"foo" + label[foo]:"bar") | (label!~:"foo" + annotation~:"foo" + annotation[foo]:"bar")`,
		},
		{
			name:  "MultiDepth Groups",
			input: `namespace: "kubecost" | ((services~:"foo" | (cluster:"cluster-one" + controllerKind:"deployment") | namespace:"bar","test") + cluster~:"cluster-")`,
		},
		{
			name: "Long Query",
			input: `
				namespace:"kubecost" +
				(label[app]:"cost_analyzer" +
				annotation[a1]:"b2" +
				cluster:"cluster-one") +
				node!:
				"node-123",
				"node-456" +
				controllerName:
				"kubecost-cost-analyzer",
				"kubecost-prometheus-server" +
				controllerKind!:
				"daemonset",
				"statefulset",
				"job" +
				container!:"123-abc_foo" +
				pod!:"aaaaaaaaaaaaaaaaaaaaaaaaa" +
				services~:"abc123" + 
				owner!:"kubecost"
			`,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d:%s", i, c.name), func(t *testing.T) {
			t.Logf("Query: %s", c.input)
			tree, err := parser.Parse(c.input)
			if err != nil {
				t.Fatalf("Unexpected parse error: %s", err)
			}
			t.Logf("%s", ast.ToPreOrderString(tree))
		})
	}
}

func TestFailingParses(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		errors int
	}{
		{
			name:   "Empty Parens",
			input:  `()`,
			errors: 1,
		},
		{
			name:   "Invalid Op",
			input:  `namespace.:"kubecost"`,
			errors: 1,
		},
		{
			name:   "Extra Closing Paren",
			input:  `(namespace:"kubecost"))`,
			errors: 1,
		},
		{
			name:   "Extra Opening Paren",
			input:  `((namespace:"kubecost")`,
			errors: 1,
		},
		{
			name:   "Or And Mixing",
			input:  `namespace:"kubecost" | services~:"foo" + cluster:"bar"`,
			errors: 1,
		},
		{
			name:   "And Or Mixing",
			input:  `namespace:"kubecost" + services~:"foo" | cluster:"bar"`,
			errors: 1,
		},
		{
			name:   "And Or Mixing With Extra Closing Paren",
			input:  `(namespace:"kubecost" + (services~:"foo" | cluster:"bar") | controllerKind<~:"dep"))`,
			errors: 2,
		},
		// NOTE: This test includes coverage for an extra closing paren _early_, which basically enforces an
		// NOTE: early return. Scoping errors don't allow the parser to continue collecting errors.
		{
			name:   "And Or Mixing With Extra Early Closing Paren",
			input:  `(namespace:"kubecost" + (services~:"foo" | cluster:"bar")) | controllerKind<~:"dep")`,
			errors: 1,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d:%s", i, c.name), func(t *testing.T) {
			t.Logf("Query: %s", c.input)
			tree, err := parser.Parse(c.input)
			if err == nil {
				t.Fatalf("Expected parsing failure. Instead, got a valid tree: \n%s\n", ast.ToPreOrderString(tree))
			}

			t.Logf("Errors: %s\n", err)

			mErr := errors.Unwrap(err)
			totalErrors := len(mErr.(*multierror.Error).Errors)
			if totalErrors != c.errors {
				t.Fatalf("Expected %d errors from parsing. Got %d", c.errors, totalErrors)
			}
		})
	}
}

func TestShortPrint(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name: "Empty",
			input: `              
			
			`,
		},
		{
			name:  "Single",
			input: `namespace: "kubecost"`,
		},
		{
			name:  "Single Group",
			input: `(namespace: "kubecost")`,
		},
		{
			name:  "Single Double Group",
			input: `((namespace: "kubecost"))`,
		},
		{
			name:  "And 2x Expression",
			input: `(namespace: "kubecost" + services~:"foo")`,
		},
		{
			name:  "And 4x Expression",
			input: `(namespace: "kubecost" + services~:"foo" + cluster:"cluster-one" + controllerKind:"deployment")`,
		},
		{
			name:  "Nested And Groups",
			input: `namespace: "kubecost" + services~:"foo" + (cluster:"cluster-one" + controllerKind:"deployment")`,
		},
		{
			name:  "Nested Or Groups",
			input: `namespace: "kubecost" | services~:"foo" | (cluster:"cluster-one" | controllerKind:"deployment")`,
		},
		{
			name:  "Nested AndOr Groups",
			input: `namespace: "kubecost" + services~:"foo" + (cluster:"cluster-one" | controllerKind:"deployment")`,
		},
		{
			name:  "Nested OrAnd Groups",
			input: `namespace: "kubecost" | services~:"foo" | (cluster:"cluster-one" + controllerKind:"deployment")`,
		},
		{
			name:  "Nested OrAndOr Groups",
			input: `namespace: "kubecost" | services~:"foo" | (cluster:"cluster-one" + controllerKind:"deployment") | namespace:"bar","test"`,
		},
		{
			name:  "Non-uniform Whitespace",
			input: `node:"node a b c" , "node 12 3"` + string('\n') + "+" + string('\n') + string('\r') + `namespace : "kubecost"`,
		},
		{
			name:  "Group Or Comparison",
			input: `(namespace:"kubecost" | cluster<~:"cluster-") + services~:"foo"`,
		},
		{
			name:  "Group Or Group",
			input: `(label~:"foo" + label[foo]:"bar") | (label!~:"foo" + annotation~:"foo" + annotation[foo]:"bar")`,
		},
		{
			name:  "MultiDepth Groups",
			input: `namespace: "kubecost" | ((services~:"foo" | (cluster:"cluster-one" + controllerKind:"deployment") | namespace:"bar","test") + cluster~:"cluster-")`,
		},
		{
			name: "Long Query",
			input: `
				namespace:"kubecost" +
				(label[app]:"cost_analyzer" +
				annotation[a1]:"b2" +
				cluster:"cluster-one") +
				node!:
				"node-123",
				"node-456" +
				controllerName:
				"kubecost-cost-analyzer",
				"kubecost-prometheus-server" +
				controllerKind!:
				"daemonset",
				"statefulset",
				"job" +
				container!:"123-abc_foo" +
				pod!:"aaaaaaaaaaaaaaaaaaaaaaaaa" +
				services~:"abc123" + 
				owner!:"kubecost"
			`,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d:%s", i, c.name), func(t *testing.T) {
			t.Logf("Query: %s", c.input)
			tree, err := parser.Parse(c.input)
			if err != nil {
				t.Fatalf("Unexpected parse error: %s", err)
			}
			t.Logf("%s", ast.ToPreOrderShortString(tree))
		})
	}
}
