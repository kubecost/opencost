package ast

import (
	"testing"
)

var allocFields map[string]*Field = map[string]*Field{
	"cluster":        NewField("cluster"),
	"node":           NewField("node"),
	"namespace":      NewField("namespace"),
	"controllerName": NewField("controllerName"),
	"controllerKind": NewField("controllerKind"),
	"container":      NewField("container"),
	"pod":            NewField("pod"),
	"services":       NewSliceField("services"),
}

var allocMapFields map[string]*Field = map[string]*Field{
	"label":      NewMapField("label"),
	"annotation": NewMapField("annotation"),
}

func TestLexerGroup(t *testing.T) {
	tokens, err := lex(
		`cluster:"cluster-one"+namespace:"kubecost"+(controllerKind!:"daemonset","deployment")+controllerName:"kubecost-network-costs"+container:"kubecost-network-costs"`,
		allocFields,
		allocMapFields)

	if err != nil {
		t.Errorf("Error: %s", err)
	}

	for _, token := range tokens {
		t.Logf("%s", token)
	}
}

func TestLexer(t *testing.T) {
	cases := []struct {
		name string

		input       string
		expectError bool
		expected    []token
	}{
		{
			name:     "Empty string",
			input:    "",
			expected: []token{{kind: eof}},
		},
		{
			name:     "colon",
			input:    ":",
			expected: []token{{kind: colon, s: ":"}, {kind: eof}},
		},
		{
			name:     "comma",
			input:    ",",
			expected: []token{{kind: comma, s: ","}, {kind: eof}},
		},
		{
			name:     "plus",
			input:    "+",
			expected: []token{{kind: plus, s: "+"}, {kind: eof}},
		},
		{
			name:     "or",
			input:    "|",
			expected: []token{{kind: or, s: "|"}, {kind: eof}},
		},
		{
			name:     "bangColon",
			input:    "!:",
			expected: []token{{kind: bangColon, s: "!:"}, {kind: eof}},
		},
		{
			name:     "tildeColon",
			input:    "~:",
			expected: []token{{kind: tildeColon, s: "~:"}, {kind: eof}},
		},
		{
			name:     "bangTildeColon",
			input:    "!~:",
			expected: []token{{kind: bangTildeColon, s: "!~:"}, {kind: eof}},
		},
		{
			name:     "startTildeColon",
			input:    "<~:",
			expected: []token{{kind: startTildeColon, s: "<~:"}, {kind: eof}},
		},
		{
			name:     "bangStartTildeColon",
			input:    "!<~:",
			expected: []token{{kind: bangStartTildeColon, s: "!<~:"}, {kind: eof}},
		},
		{
			name:     "tildeEndColon",
			input:    "~>:",
			expected: []token{{kind: tildeEndColon, s: "~>:"}, {kind: eof}},
		},
		{
			name:     "bangTildeEndColon",
			input:    "!~>:",
			expected: []token{{kind: bangTildeEndColon, s: "!~>:"}, {kind: eof}},
		},
		{
			name: "multiple symbols",
			// This is a valid string to parse but not to lex
			input:    "!::,+",
			expected: []token{{kind: bangColon, s: "!:"}, {kind: colon, s: ":"}, {kind: comma, s: ","}, {kind: plus, s: "+"}, {kind: eof}},
		},
		{
			name:     "string",
			input:    `"test"`,
			expected: []token{{kind: str, s: `test`}, {kind: eof}},
		},
		{
			name:     "keyed access",
			input:    "[app]",
			expected: []token{{kind: keyedAccess, s: "app"}, {kind: eof}},
		},
		{
			name:     "identifier pure alpha",
			input:    "abc",
			expected: []token{{kind: identifier, s: "abc"}, {kind: eof}},
		},
		{
			name:     "label access",
			input:    "app[kubecost]",
			expected: []token{{kind: identifier, s: "app"}, {kind: keyedAccess, s: "kubecost"}, {kind: eof}},
		},
		{
			name:  "whitespace variety",
			input: "1 2" + string('\n') + `" ` + string('\n') + string('\t') + string('\r') + `a"` + string('\t') + string('\r') + "abc[foo a]" + " ",
			expected: []token{
				{kind: identifier, s: "1"},
				{kind: identifier, s: "2"},
				{kind: str, s: " " + string('\n') + string('\t') + string('\r') + "a"},
				{kind: identifier, s: "abc"},
				{kind: keyedAccess, s: "foo a"},
				{kind: eof},
			},
		},
		{
			name:  "whitespace separated accesses",
			input: `node : "abc" , "def" ` + string('\r') + string('\n') + string('\t') + `namespace : "123"`,
			expected: []token{
				{kind: filterField, s: "node"},
				{kind: colon, s: ":"},
				{kind: str, s: "abc"},
				{kind: comma, s: ","},
				{kind: str, s: "def"},
				{kind: filterField, s: "namespace"},
				{kind: colon, s: ":"},
				{kind: str, s: "123"},
				{kind: eof},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Logf("Input: '%s'", c.input)
			result, err := lex(c.input, allocFields, allocMapFields)
			if c.expectError && err == nil {
				t.Errorf("expected error but got nil")
			} else if !c.expectError && err != nil {
				t.Errorf("unexpected error: %s", err)
			} else {
				if len(c.expected) != len(result) {
					t.Fatalf("Token slices don't match in length.\nExpected: %+v\nGot: %+v", c.expected, result)
				}
				for i := range c.expected {
					if c.expected[i] != result[i] {
						t.Fatalf("Incorrect token at position %d.\nExpected: %+v\nGot: %+v", i, c.expected, result)
					}
				}
			}
		})
	}
}
