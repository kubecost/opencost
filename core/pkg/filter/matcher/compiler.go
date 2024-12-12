package matcher

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/transform"
	"github.com/opencost/opencost/core/pkg/filter/util"
)

// FieldMapper is the adapter which can fetch actual T instance data of type U
// leveraging the ast.Identifier definition.
type FieldMapper[T any, U any] func(T, ast.Identifier) (U, error)

// StringFieldMapper is the adapter which can fetch actual T instance data of type string
// leveraging the ast.Identifier definition.
type StringFieldMapper[T any] FieldMapper[T, string]

// SliceFieldMapper is the adapter which can fetch actual T instance data of type []string
// leveraging the ast.Identifier definition.
type SliceFieldMapper[T any] FieldMapper[T, []string]

// SliceFieldMapper is the adapter which can fetch actual T instance data of type map[string]string
// leveraging the ast.Identifier definition.
type MapFieldMapper[T any] FieldMapper[T, map[string]string]

// MatchCompiler compiles an `ast.FilterNode` into a Matcher[T] implementation.
type MatchCompiler[T any] struct {
	stringMatcher *StringMatcherFactory[T]
	sliceMatcher  *StringSliceMatcherFactory[T]
	mapMatcher    *StringMapMatcherFactory[T]
	passes        []transform.CompilerPass
}

// NewMatchCompiler creates a new MatchCompiler for T instances provided the funcs which
// can map ast.Identifier instances to a specific T field
func NewMatchCompiler[T any](
	stringFieldMapper StringFieldMapper[T],
	sliceFieldMapper SliceFieldMapper[T],
	mapFieldMapper MapFieldMapper[T],
	passes ...transform.CompilerPass,
) *MatchCompiler[T] {
	return &MatchCompiler[T]{
		stringMatcher: NewStringMatcherFactory(stringFieldMapper),
		sliceMatcher:  NewStringSliceMatcherFactory(sliceFieldMapper),
		mapMatcher:    NewStringMapMatcherFactory(mapFieldMapper),
		passes:        passes,
	}
}

// Compile accepts an `ast.FilterNode` tree and compiles it into a `Matcher[T]` implementation
// which can be used to match T instances dynamically.
func (mc *MatchCompiler[T]) Compile(filter ast.FilterNode) (Matcher[T], error) {
	// apply compiler passes on parsed ast
	var err error
	filter, err = transform.ApplyAll(filter, mc.passes)
	if err != nil {
		return nil, fmt.Errorf("applying compiler passes: %w", err)
	}

	// if the root node is a void op, return an allpass
	if _, ok := filter.(*ast.VoidOp); ok {
		return &AllPass[T]{}, nil
	}

	var result Matcher[T]
	var currentOps *util.Stack[MatcherGroup[T]] = util.NewStack[MatcherGroup[T]]()

	// handle leaf is the ast walker func. group ops get pushed onto a stack on
	// the Enter state, and popped on the Exit state. Any ops between Enter and
	// Exit are added to the group. If there are no more groups on the stack after
	// an Exit state, we set the result to the final group.
	handleLeaf := func(leaf ast.FilterNode, state ast.TraversalState) {
		switch n := leaf.(type) {
		case *ast.AndOp:
			if state == ast.TraversalStateEnter {
				currentOps.Push(&And[T]{})
			} else if state == ast.TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}
		case *ast.OrOp:
			if state == ast.TraversalStateEnter {
				currentOps.Push(&Or[T]{})
			} else if state == ast.TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}

		case *ast.NotOp:
			if state == ast.TraversalStateEnter {
				currentOps.Push(&Not[T]{})
			} else if state == ast.TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}
		case *ast.ContradictionOp:
			if currentOps.Length() == 0 {
				result = &AllCut[T]{}
			} else {
				currentOps.Top().Add(&AllCut[T]{})
			}
		case *ast.EqualOp:
			sm := mc.stringMatcher.NewStringMatcher(n.Op(), n.Left, n.Right)
			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}

		case *ast.ContainsOp:
			f := n.Left.Field
			key := n.Left.Key

			var sm Matcher[T]
			if f.IsSlice() {
				sm = mc.sliceMatcher.NewStringSliceMatcher(n.Op(), n.Left, n.Right)
			} else if f.IsMap() && key == "" {
				sm = mc.mapMatcher.NewStringMapMatcher(n.Op(), n.Left, n.Right)
			} else {
				sm = mc.stringMatcher.NewStringMatcher(n.Op(), n.Left, n.Right)
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}

		case *ast.ContainsPrefixOp:
			f := n.Left.Field
			key := n.Left.Key

			var sm Matcher[T]
			if f.IsSlice() {
				sm = mc.sliceMatcher.NewStringSliceMatcher(n.Op(), n.Left, n.Right)
			} else if f.IsMap() && key == "" {
				sm = mc.mapMatcher.NewStringMapMatcher(n.Op(), n.Left, n.Right)
			} else {
				sm = mc.stringMatcher.NewStringMatcher(n.Op(), n.Left, n.Right)
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}

		case *ast.ContainsSuffixOp:
			f := n.Left.Field
			key := n.Left.Key

			var sm Matcher[T]
			if f.IsSlice() {
				sm = mc.sliceMatcher.NewStringSliceMatcher(n.Op(), n.Left, n.Right)
			} else if f.IsMap() && key == "" {
				sm = mc.mapMatcher.NewStringMapMatcher(n.Op(), n.Left, n.Right)
			} else {
				sm = mc.stringMatcher.NewStringMatcher(n.Op(), n.Left, n.Right)
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}
		}
	}

	ast.PreOrderTraversal(filter, handleLeaf)
	if result == nil {
		return &AllPass[T]{}, nil
	}

	return result, nil
}
