package ast

import (
	"fmt"

	multierror "github.com/hashicorp/go-multierror"
)

// ============================================================================
// This file contains:
// Lexing (string -> []token) for V2 of allocation filters
// ============================================================================
//
// See parser.go for a formal grammar and external links.

type tokenKind int

const (
	colon tokenKind = iota // ':'
	comma                  // ','
	plus                   // '+'
	or                     // '|'

	bangColon           // '!:'
	tildeColon          // '~:'
	bangTildeColon      // '!~:'
	startTildeColon     // '<~:'
	bangStartTildeColon // '!<~:'
	tildeEndColon       // '~>:'
	bangTildeEndColon   // '!~>:'

	parenOpen  // '('
	parenClose // ')'

	str // '"foo"'

	filterField // 'namespace', 'cluster'
	mapField    // 'label', 'annotation'
	keyedAccess // '[app]', '[foo]', etc.
	identifier  // K8s valid name + sanitized Prom: 'app', 'abc_label'

	eof
)

func (tk tokenKind) String() string {
	switch tk {
	case colon:
		return "colon"
	case comma:
		return "comma"
	case plus:
		return "plus"
	case or:
		return "or"
	case bangColon:
		return "bangColon"
	case tildeColon:
		return "tildeColon"
	case bangTildeColon:
		return "bangTildeColon"
	case startTildeColon:
		return "startTildeColon"
	case bangStartTildeColon:
		return "bangStartTildeColon"
	case tildeEndColon:
		return "tildeEndColon"
	case bangTildeEndColon:
		return "bangTildeEndColon"
	case parenOpen:
		return "parenOpen"
	case parenClose:
		return "parenClose"
	case str:
		return "str"
	case filterField:
		return "filterField1"
	case mapField:
		return "filterField2"
	case keyedAccess:
		return "keyedAccess"
	case identifier:
		return "identifier"
	case eof:
		return "eof"
	default:
		return fmt.Sprintf("Unspecified: %d", tk)
	}
}

// ============================================================================
// Lexer/Scanner
//
// Based on the Scanner class in Chapter 4: Scanning of Crafting Interpreters by
// Robert Nystrom
// ============================================================================

type token struct {
	kind tokenKind
	s    string
}

func (t token) String() string {
	return fmt.Sprintf("%s:%s", t.kind, t.s)
}

type scanner struct {
	source string
	tokens []token
	errors []error

	fields    map[string]*Field
	mapFields map[string]*Field

	lexemeStartByte int
	nextByte        int
}

func (s *scanner) scanTokens() {
	for !s.atEnd() {
		s.lexemeStartByte = s.nextByte
		s.scanToken()
	}

	s.tokens = append(s.tokens, token{kind: eof})
}

func (s scanner) atEnd() bool {
	return s.nextByte >= len(s.source)
}

// advance returns a byte because we only accept ASCII, which has to fit in a
// byte
//
// TODO: If we add unicode support, advance() will probably have to return a
// rune.
func (s *scanner) advance() byte {
	b := s.source[s.nextByte]
	s.nextByte += 1
	return b
}

func (s *scanner) match(expected byte) bool {
	if s.atEnd() {
		return false
	}
	if s.source[s.nextByte] != expected {
		return false
	}
	s.nextByte += 1
	return true
}

func (s *scanner) addToken(kind tokenKind) {
	lexemeString := s.source[s.lexemeStartByte:s.nextByte]
	switch kind {
	// Eliminate surrounding characters like " and []
	case str, keyedAccess:
		lexemeString = lexemeString[1 : len(lexemeString)-1]
	}

	s.tokens = append(s.tokens, token{
		kind: kind,
		s:    lexemeString,
	})
}

func (s *scanner) peek() byte {
	if s.atEnd() {
		return 0
	}
	return s.source[s.nextByte]
}

func (s *scanner) scanToken() {
	c := s.advance()
	switch c {
	case ':':
		s.addToken(colon)
	case ',':
		s.addToken(comma)
	case '+':
		s.addToken(plus)
	case '|':
		s.addToken(or)
	case '!':
		if s.match(':') {
			s.addToken(bangColon)
		} else if s.match('~') {
			if s.match(':') {
				s.addToken(bangTildeColon)
			} else if s.match('>') {
				if s.match(':') {
					s.addToken(bangTildeEndColon)
				} else {
					s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '>'", s.nextByte-1))
				}
			} else {
				s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '~'", s.nextByte-1))
			}
		} else if s.match('<') {
			if s.match('~') {
				if s.match(':') {
					s.addToken(bangStartTildeColon)
				} else {
					s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '~'", s.nextByte-1))
				}
			} else {
				s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '<'", s.nextByte-1))
			}
		} else {
			s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '!'", s.nextByte-1))
		}
	case '(':
		s.addToken(parenOpen)
	case ')':
		s.addToken(parenClose)
	case '<':
		if s.match('~') {
			if s.match(':') {
				s.addToken(startTildeColon)
			} else {
				s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '~'", s.nextByte-1))
			}
		} else {
			s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '<'", s.nextByte-1))
		}
	case '~':
		if s.match(':') {
			s.addToken(tildeColon)
		} else if s.match('>') {
			if s.match(':') {
				s.addToken(tildeEndColon)
			} else {
				s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '>'", s.nextByte-1))
			}
		} else {
			s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '~'", s.nextByte-1))
		}
	// strings
	case '"':
		s.string()
	// keyed access
	case '[':
		s.keyedAccess()
	// Ignore whitespace chars outside of "" and [].
	case ' ', '\t', '\n', '\r':
		break
	default:
		// identifiers
		//
		// We can keep it simple and not _force_ the first character to be a
		// non-number because we don't need numbers in this language. If we need
		// to extend the language to support numbers, this has to become just
		// isAlpha() and then s.identifier() will use isIdentifierChar() in
		// its main loop.
		if isIdentifierChar(c) {
			s.identifier()
			break
		}

		// TODO: We could return a more exact error message for Unicode chars if
		// we added extra handling:
		// https://stackoverflow.com/questions/53069040/checking-a-string-contains-only-ascii-characters
		s.errors = append(s.errors, fmt.Errorf("unexpected character/byte at position %d. Please avoid Unicode.", s.nextByte-1))
	}
}

// isIdentifierChar should match Kubernetes-supported name characters.
//
// https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
//
// TODO: This may not match all characters we support for cluster IDs (it may be
// the case that cluster IDs can contain UTF-8 characters).
func isIdentifierChar(b byte) bool {
	return (b >= '0' && b <= '9') || // 0-9
		(b >= 'A' && b <= 'Z') || // A-Z
		(b >= 'a' && b <= 'z') || // a-z
		b == '-' || // hyphens are allowed according to K8s spec
		b == '_' // underscores are allowed because of Prometheus sanitization
}

func (s *scanner) string() {
	for s.peek() != '"' && !s.atEnd() {
		s.advance()
	}

	if s.atEnd() {
		s.errors = append(s.errors, fmt.Errorf("unterminated string starting at %d", s.lexemeStartByte))
		return
	}

	// Consume closing '"'
	s.advance()

	s.addToken(str)
}

func (s *scanner) keyedAccess() {
	for s.peek() != ']' && !s.atEnd() {
		s.advance()
	}

	if s.atEnd() {
		s.errors = append(s.errors, fmt.Errorf("unterminated access starting at %d", s.lexemeStartByte))
		return
	}

	// Consume closing ']'
	s.advance()
	s.addToken(keyedAccess)
}

func (s *scanner) identifier() {
	for isIdentifierChar(s.peek()) {
		s.advance()
	}

	tokenText := s.source[s.lexemeStartByte:s.nextByte]
	if _, ok := s.fields[tokenText]; ok {
		s.addToken(filterField)
	} else if _, ok := s.mapFields[tokenText]; ok {
		s.addToken(mapField)
	} else {
		s.addToken(identifier)
	}
}

// lex will generate a slice of tokens provided a raw string and the filter field definitions
func lex(raw string, fields map[string]*Field, mapFields map[string]*Field) ([]token, error) {
	s := scanner{
		source:    raw,
		fields:    fields,
		mapFields: mapFields,
	}
	s.scanTokens()

	if len(s.errors) > 0 {
		return s.tokens, multierror.Append(nil, s.errors...)
	}

	return s.tokens, nil
}
