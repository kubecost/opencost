// allocationfilterutil provides functionality for parsing V2 of the Kubecost
// filter language for Allocation types.
//
// e.g. "filter=namespace:kubecost+controllerkind:deployment"
package ast

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
)

// The grammar is approximately as follows:
//
// <filter>         ::= <filter-element> (<group-op> <filter-element>)*
// <filter-element> ::= <comparison> | <group-filter>
// <group-filter>   ::= '(' <filter> ')'
// <group-op>       ::= '+' | '|'
// <comparison>     ::= <filter-key> <filter-op> <filter-value>
// <filter-key>     ::= <map-field> <keyed-access> | <filter-field>
// <filter-op>      ::= ':' | '!:' | '~:' | '!~:' | '<~:' | '!<~:' | '~>:' | '!~>:'
// <filter-value>   ::= '"' <identifier> '"' (',' <filter-value>)*
// <keyed-access>   ::= '[' <identifier> ']'
// <map-field>      ::= --- (fields passed into lexer)
// <filter-field>   ::= --- (fields passed into lexer)
// <identifier>     ::= --- valid K8s name or Prom-sanitized K8s name

// ============================================================================
// Parser
//
// Based on the Parser class in Chapter 6: Parsing Expressions of Crafting
// Interpreters by Robert Nystrom
// ============================================================================

// parseError produces error messages tailored to the needs of the parser
func parseError(t token, message string) error {
	if t.kind == eof {
		return fmt.Errorf("at end: %s", message)
	}

	return fmt.Errorf("at '%s': %s", t.s, message)
}

type parser struct {
	tokens  []token
	current int

	fields    map[string]*Field
	mapFields map[string]*Field
}

// ----------------------------------------------------------------------------
// Parser helper methods for token handling
// ----------------------------------------------------------------------------

func (p *parser) atEnd() bool {
	return p.peek().kind == eof
}

func (p *parser) advance() token {
	if !p.atEnd() {
		p.current += 1
	}

	return p.previous()
}

func (p *parser) previous() token {
	return p.tokens[p.current-1]
}

// match return true and advances the parser by one token if the next token has
// a kind that matches one of the arguments. Otherwise, it returns false and
// DOES NOT advance the parser.
func (p *parser) match(tokenKinds ...tokenKind) bool {
	for _, kind := range tokenKinds {
		if p.check(kind) {
			p.advance()
			return true
		}
	}
	return false
}

// check returns true iff the next token matches the provided kind.
func (p *parser) check(tk tokenKind) bool {
	if p.atEnd() {
		return false
	}
	return p.peek().kind == tk
}

func (p *parser) peek() token {
	return p.tokens[p.current]
}

// consume is a "next token must be this kind" method. If the next token is of
// the correct kind, the parser is advanced and that token is returned. If it
// is not of the correct kind, a parse error is returned and the parser is NOT
// advanced.
func (p *parser) consume(tk tokenKind, message string) (token, error) {
	if p.check(tk) {
		return p.advance(), nil
	}

	return token{}, parseError(p.peek(), message)
}

// synchronize attempts to skip forward until the next tokenKind, indicating the
// start of a new (plus, or, or parenClose).
func (p *parser) synchronize(tokens ...tokenKind) {
	if len(tokens) == 0 {
		return
	}

	for !p.atEnd() {
		kind := p.peek().kind
		for _, token := range tokens {
			if kind == token {
				return
			}
		}

		p.advance()
	}
}

// ----------------------------------------------------------------------------
// Parser grammar rules as recursive descent methods
// ----------------------------------------------------------------------------

// filter is the main method of the parser. It turns the token stream into an
// FilterNode tree, reporting parse errors that occurred along the way. The depth
// parameter is the number of edges from the node to the tree's root node, which
// is initially 0. As we recurse into the tree, the depth will increase.
func (p *parser) filter(depth int) (FilterNode, error) {
	var errs *multierror.Error

	// ----------------------------------------------------------------------------
	//  Capture Starting Op
	// ----------------------------------------------------------------------------
	// Since every valid filter starts with an operand, this is always our first
	// step. Depending on the _next_ token, we can either stop here or use a grouping
	// operator (+ or |).
	var top FilterNode

	// If we determine after parsing the first op that we have a group op, we'll create
	// the group based on the operator and push the top into the group.
	var f FilterGroup = nil

	// Special Case: Empty Filter on depth = 0 and first token is eof
	if depth == 0 && p.peek().kind == eof {
		return &VoidOp{}, errs.ErrorOrNil()
	}

	// Open Paren indicates a new filter depth, so we recursively call filter with depth+1.
	if p.match(parenOpen) {
		node, err := p.filter(depth + 1)
		if err != nil {
			errs = multierror.Append(errs, err)
		} else {
			top = node
		}
	} else {
		comparison, err := p.comparison()
		if err != nil {
			errs = multierror.Append(errs, err)
			p.synchronize(plus, or, parenClose)
		} else {
			top = comparison
		}
	}

	// Handles case `( <comparison> )` with no grouping ops.
	if p.match(parenClose) {
		if depth <= 0 {
			errs = multierror.Append(errs, fmt.Errorf("Found ')' without matching '('"))
		}

		return top, errs.ErrorOrNil()
	}

	// ----------------------------------------------------------------------------
	//  Determine Group Operator
	// ----------------------------------------------------------------------------
	// Once we land here, we expect an operator as the next token. This operator will
	// determine the group for this scope and be used to continue parsing as long as
	// the operators following the initial are _the same_.
	//
	// For instance:
	// ( <comparison> | <comparison> | <comparison> ) is allowed
	// ( <comparison> + <comparison> + (<comparison> | <comparison>)) is allowed
	// ( <comparison> | <comparison> + <comparison> ) is _NOT_ allowed

	// Create the proper grouping operator based on the current token kind,
	// then use a while to capture each repition of the _same_ operator.
	selectedOp := p.peek().kind
	if selectedOp == plus || selectedOp == or {
		if selectedOp == plus {
			f = &AndOp{}
		} else if selectedOp == or {
			f = &OrOp{}
		}

		// Once we determine we are using a group operator, it's safe to push
		// the current top level operand into the group
		f.Add(top)

		// Capture each repetition
		for p.match(selectedOp) {
			if p.match(parenOpen) {
				node, err := p.filter(depth + 1)
				if err != nil {
					errs = multierror.Append(errs, err)
				} else {
					f.Add(node)
				}
			} else {
				right, err := p.comparison()
				if err != nil {
					errs = multierror.Append(errs, err)
					p.synchronize(plus, or, parenClose)
				} else {
					f.Add(right)
				}
			}

			if p.match(parenClose) {
				if depth <= 0 {
					errs = multierror.Append(errs, fmt.Errorf("Found ')' without matching '('"))
				}

				return f, errs.ErrorOrNil()
			}

			// The following code enforces continued use of a single operator within a scope.
			// ie: (a | b + c) is disallowed
			//
			// In order to continue parsing (to continue to collect parse errors), we need to fast-
			// forward to the next instance of an operator or scope close.
			nextOp := p.peek().kind
			if nextOp != eof && nextOp != selectedOp {
				errs = multierror.Append(errs, fmt.Errorf("Found \"%s\", Expected \"%s\"", nextOp.String(), selectedOp.String()))
				// since we were peeking for this check, to correctly synchronize, we must advance at least once
				p.advance()
				p.synchronize(plus, or, parenClose)

				// since it's possible to synchronize to a paren close, we need to ensure we correctly pop the
				// current scope if that's the case.
				if p.match(parenClose) {
					return f, errs.ErrorOrNil()
				}
			}
		}
	}

	// It should not be possible to reach this point on a non-zero depth, so we
	// must have a () mismatch
	if depth > 0 {
		errs = multierror.Append(errs, fmt.Errorf("Found '(' without matching ')'"))
	}

	// If we didn't have a grouping operator, we simply return the single op
	if f == nil {
		return top, errs.ErrorOrNil()
	}

	return f, errs.ErrorOrNil()
}

func (p *parser) comparison() (FilterNode, error) {
	field, key, err := p.filterKey()
	if err != nil {
		return nil, err
	}

	opToken, err := p.filterOp()
	if err != nil {
		return nil, err
	}

	var op FilterOp

	switch opToken.kind {
	case colon:
		// for ':' using a slice or key-less map, treat as '~:'
		if field.IsSlice() || (field.IsMap() && key == "") {
			op = FilterOpContains
		} else {
			op = FilterOpEquals
		}
	case bangColon:
		// for '!:' using a slice or key-less map, treat as '!~:'
		if field.IsSlice() || (field.IsMap() && key == "") {
			op = FilterOpNotContains
		} else {
			op = FilterOpNotEquals
		}
	case tildeColon:
		op = FilterOpContains
	case bangTildeColon:
		op = FilterOpNotContains
	case startTildeColon:
		op = FilterOpContainsPrefix
	case bangStartTildeColon:
		op = FilterOpNotContainsPrefix
	case tildeEndColon:
		op = FilterOpContainsSuffix
	case bangTildeEndColon:
		op = FilterOpNotContainsSuffix
	default:
		return nil, parseError(opToken, "implementation problem: unhandled op token")
	}

	values, err := p.filterValues()
	if err != nil {
		return nil, err
	}

	switch opToken.kind {
	// In the != case, a sequence of filter values is ANDed
	// Example:
	// namespace!:"foo","bar" -> (and (notequals namespace foo)
	//                                (notequals namespace bar))
	case bangColon, bangTildeColon, bangStartTildeColon, bangTildeEndColon:
		// Only a single filter value, don't need to wrap in AND
		if len(values) == 1 {
			node, err := toFilterNode(field, key, op, values[0])
			if err != nil {
				return nil, fmt.Errorf("Parse Error: %s", err)
			}

			return node, nil
		}

		// Multiple filter values, wrap in AND
		baseFilter := &AndOp{}
		for _, v := range values {
			node, err := toFilterNode(field, key, op, v)
			if err != nil {
				return nil, fmt.Errorf("Parse Error: %s", err)
			}

			baseFilter.Operands = append(baseFilter.Operands, node)
		}

		return baseFilter, nil

	default:
		// Only a single filter value, don't need to wrap in OR
		if len(values) == 1 {
			node, err := toFilterNode(field, key, op, values[0])
			if err != nil {
				return nil, fmt.Errorf("Parse Error: %s", err)
			}

			return node, nil
		}

		// Multiple filter values, wrap in OR
		baseFilter := &OrOp{}
		for _, v := range values {
			node, err := toFilterNode(field, key, op, v)
			if err != nil {
				return nil, fmt.Errorf("Parse Error: %s", err)
			}

			baseFilter.Operands = append(baseFilter.Operands, node)
		}

		return baseFilter, nil
	}

}

// filterKey parses a series of tokens that represent a "filter key", returning
// an error if a filter key cannot be constructed.
//
// Examples:
// tokens = [filterField2:label keyedAccess:app] -> FilterLabel, app, nil
// tokens = [filterField1:namespace] -> FilterNamespace, "", nil
func (p *parser) filterKey() (field *Field, key string, err error) {
	if p.match(mapField) {
		rawField := p.previous().s
		mappedField, ok := p.mapFields[rawField]
		if !ok {
			return nil, "", parseError(p.previous(), "expect key-mapped filter field, like 'label' or 'annotation'")
		}

		// keyed-access is optional after a map field
		if p.match(keyedAccess) {
			key = p.previous().s
		} else {
			key = ""
		}

		return mappedField, key, nil
	}

	_, err = p.consume(filterField, "expect filter field")
	if err != nil {
		return nil, "", err
	}

	rawField := p.previous().s
	mappedField, ok := p.fields[rawField]
	if !ok {
		return nil, "", parseError(p.previous(), "expect known filter field, like 'cluster' or 'namespace'")
	}

	return mappedField, "", nil
}

func (p *parser) filterOp() (token, error) {
	if p.match(colon, bangColon, tildeColon, bangTildeColon, startTildeColon, bangStartTildeColon, tildeEndColon, bangTildeEndColon) {
		return p.previous(), nil
	}

	return token{}, parseError(p.peek(), "expect filter op like ':', '!:', '~:', or '!~:'")
}

func (p *parser) filterValues() ([]string, error) {
	vals := []string{}

	_, err := p.consume(str, "expect string as filter value")
	if err != nil {
		return nil, err
	}
	vals = append(vals, p.previous().s)

	for p.match(comma) {
		_, err := p.consume(str, "expect string as filter value")
		if err != nil {
			return nil, err
		}

		vals = append(vals, p.previous().s)
	}

	return vals, nil
}

func toFilterNode(field *Field, key string, op FilterOp, value string) (FilterNode, error) {
	switch op {
	case FilterOpEquals:
		return &EqualOp{
			Left: Identifier{
				Field: field,
				Key:   key,
			},
			Right: value,
		}, nil

	case FilterOpNotEquals:
		return &NotOp{
			Operand: &EqualOp{
				Left: Identifier{
					Field: field,
					Key:   key,
				},
				Right: value,
			},
		}, nil

	case FilterOpContains:
		return &ContainsOp{
			Left: Identifier{
				Field: field,
				Key:   key,
			},
			Right: value,
		}, nil

	case FilterOpNotContains:
		return &NotOp{
			Operand: &ContainsOp{
				Left: Identifier{
					Field: field,
					Key:   key,
				},
				Right: value,
			},
		}, nil

	case FilterOpContainsPrefix:
		return &ContainsPrefixOp{
			Left: Identifier{
				Field: field,
				Key:   key,
			},
			Right: value,
		}, nil

	case FilterOpNotContainsPrefix:
		return &NotOp{
			Operand: &ContainsPrefixOp{
				Left: Identifier{
					Field: field,
					Key:   key,
				},
				Right: value,
			},
		}, nil

	case FilterOpContainsSuffix:
		return &ContainsSuffixOp{
			Left: Identifier{
				Field: field,
				Key:   key,
			},
			Right: value,
		}, nil

	case FilterOpNotContainsSuffix:
		return &NotOp{
			Operand: &ContainsSuffixOp{
				Left: Identifier{
					Field: field,
					Key:   key,
				},
				Right: value,
			},
		}, nil

	default:
		return nil, fmt.Errorf("Failed to parse op: %s", op)
	}
}

// FilterParser is an object capable of parsing a filter string into a `FilterNode`
// AST
type FilterParser interface {
	// Parse parses a filter string into a FilterNode AST.
	Parse(filter string) (FilterNode, error)
}

// default implementation of FilterParser
type defaultFilterParser struct {
	fields    map[string]*Field
	mapFields map[string]*Field
}

// Parse parses a filter string into a FilterNode AST.
func (dfp *defaultFilterParser) Parse(filter string) (FilterNode, error) {
	tokens, err := lex(filter, dfp.fields, dfp.mapFields)
	if err != nil {
		return nil, fmt.Errorf("lexing filter: %w", err)
	}

	p := parser{
		tokens:    tokens,
		fields:    dfp.fields,
		mapFields: dfp.mapFields,
	}

	parsedFilter, err := p.filter(0)
	if err != nil {
		return nil, fmt.Errorf("parsing filter: %w", err)
	}

	return parsedFilter, nil
}

// splits a slice of Field instances into a map of fields (key'd by name) that have no key-based
// access and those that have key-based access.
func fieldsToMaps(fs []*Field) (fields map[string]*Field, mapFields map[string]*Field) {
	fields = make(map[string]*Field)
	mapFields = make(map[string]*Field)

	for _, f := range fs {
		if f.IsMap() {
			mapFields[f.Name] = f
		} else {
			fields[f.Name] = f
		}
	}
	return
}

// NewFilterParser creates a new `FilterParser` instance with the provided `Field` definitions to
// use when lexing and parsing.
func NewFilterParser(fields []*Field) FilterParser {
	f, m := fieldsToMaps(fields)

	return &defaultFilterParser{
		fields:    f,
		mapFields: m,
	}
}
