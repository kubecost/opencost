package prom

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
)

// errorType used to check HasError
var errorType = reflect.TypeOf((*error)(nil)).Elem()

// NoStoreAPIWarning is a warning that we would consider an error. It returns partial data relating only to the
// store apis which were reachable. In order to ensure integrity of data across all clusters, we'll need to identify
// this warning and convert it to an error.
const NoStoreAPIWarning string = "No StoreAPIs matched for this query"

// IsNoStoreAPIWarning checks a warning to determine if it is equivalent to a no store API query.
func IsNoStoreAPIWarning(warning string) bool {
	return strings.EqualFold(warning, NoStoreAPIWarning)
}

//--------------------------------------------------------------------------
//  Prometheus Error Collection
//--------------------------------------------------------------------------

type QueryError struct {
	Query      string `json:"query"`
	Error      error  `json:"error"`
	ParseError error  `json:"parseError"`
}

// String returns a string representation of the QueryError
func (qe *QueryError) String() string {
	var sb strings.Builder
	sb.WriteString("Errors:\n")
	if qe.Error != nil {
		sb.WriteString(fmt.Sprintf("  Request Error: %s\n", qe.Error))
	}
	if qe.ParseError != nil {
		sb.WriteString(fmt.Sprintf("  Parse Error: %s\n", qe.ParseError))
	}
	sb.WriteString(fmt.Sprintf("for Query: %s\n", qe.Query))
	return sb.String()
}

type QueryWarning struct {
	Query    string   `json:"query"`
	Warnings []string `json:"warnings"`
}

// String returns a string representation of the QueryWarning
func (qw *QueryWarning) String() string {
	var sb strings.Builder
	sb.WriteString("Warnings:\n")
	for i, w := range qw.Warnings {
		sb.WriteString(fmt.Sprintf("  %d) %s\n", i+1, w))
	}
	sb.WriteString(fmt.Sprintf("for Query: %s\n", qw.Query))
	return sb.String()
}

// QueryErrorCollection represents a collection of query errors and warnings made via context.
type QueryErrorCollection interface {
	// Warnings is a slice of the QueryWarning instances
	Warnings() []*QueryWarning

	// Errors is a slice of the QueryError instances
	Errors() []*QueryError

	// ToErrorAndWarningStrings returns the errors and warnings in the collection
	// as two string slices.
	ToErrorAndWarningStrings() (errors []string, warnings []string)
}

// ErrorsAndWarningStrings is a container struct for string representation storage/caching
type ErrorsAndWarningStrings struct {
	Errors   []string
	Warnings []string
}

// QueryErrorCollector is used to collect prometheus query errors and warnings, and also meets the
// Error interface
type QueryErrorCollector struct {
	m        sync.RWMutex
	errors   []*QueryError
	warnings []*QueryWarning
}

// Reports an error to the collector. Ignores if the error is nil and the warnings
// are empty
func (ec *QueryErrorCollector) Report(query string, warnings []string, requestError error, parseError error) {
	if requestError == nil && parseError == nil && len(warnings) == 0 {
		return
	}

	ec.m.Lock()
	defer ec.m.Unlock()

	if requestError != nil || parseError != nil {
		ec.errors = append(ec.errors, &QueryError{
			Query:      query,
			Error:      requestError,
			ParseError: parseError,
		})
	}

	if len(warnings) > 0 {
		ec.warnings = append(ec.warnings, &QueryWarning{
			Query:    query,
			Warnings: warnings,
		})
	}
}

// Whether or not the collector caught any warnings
func (ec *QueryErrorCollector) IsWarning() bool {
	ec.m.RLock()
	defer ec.m.RUnlock()

	return len(ec.warnings) > 0
}

// Whether or not the collector caught errors
func (ec *QueryErrorCollector) IsError() bool {
	ec.m.RLock()
	defer ec.m.RUnlock()

	return len(ec.errors) > 0
}

// Warnings caught by the collector
func (ec *QueryErrorCollector) Warnings() []*QueryWarning {
	ec.m.RLock()
	defer ec.m.RUnlock()

	warns := make([]*QueryWarning, len(ec.warnings))
	copy(warns, ec.warnings)
	return warns
}

// Errors caught by the collector
func (ec *QueryErrorCollector) Errors() []*QueryError {
	ec.m.RLock()
	defer ec.m.RUnlock()

	errs := make([]*QueryError, len(ec.errors))
	copy(errs, ec.errors)
	return errs
}

// Implement the error interface to allow returning as an aggregated error
func (ec *QueryErrorCollector) Error() string {
	ec.m.RLock()
	defer ec.m.RUnlock()

	var sb strings.Builder
	if len(ec.errors) > 0 {
		sb.WriteString("Error Collection:\n")
		for i, e := range ec.errors {
			sb.WriteString(fmt.Sprintf("%d) %s\n", i, e))
		}
	}
	if len(ec.warnings) > 0 {
		sb.WriteString("Warning Collection:\n")
		for _, w := range ec.warnings {
			sb.WriteString(w.String())
		}
	}

	return sb.String()
}

// ToErrorAndWarningStrings returns the errors and warnings in the collection as two string slices.
func (ec *QueryErrorCollector) ToErrorAndWarningStrings() (errors []string, warnings []string) {
	for _, e := range ec.Errors() {
		errors = append(errors, e.String())
	}
	for _, w := range ec.Warnings() {
		warnings = append(warnings, w.String())
	}
	return
}

// As is a special method that implicitly works with the `errors.As()` go
// helper to locate the _first_ instance of the provided target type in the
// collection.
func (ec *QueryErrorCollector) As(target interface{}) bool {
	if target == nil {
		log.Errorf("ErrorCollection.As() target cannot be nil")
		return false
	}

	val := reflect.ValueOf(target)
	typ := val.Type()
	if typ.Kind() != reflect.Ptr || val.IsNil() {
		log.Errorf("ErrorCollection.As() target must be a non-nil pointer")
		return false
	}
	if e := typ.Elem(); e.Kind() != reflect.Interface && !e.Implements(errorType) {
		log.Errorf("ErrorCollection.As() *target must be interface or implement error")
		return false
	}

	targetType := typ.Elem()
	for _, err := range AllErrorsFor(ec) {
		if reflect.TypeOf(err).AssignableTo(targetType) {
			val.Elem().Set(reflect.ValueOf(err))
			return true
		}
		if x, ok := err.(interface{ As(interface{}) bool }); ok && x.As(target) {
			return true
		}
	}

	return false
}

// IsErrorCollection returns true if the provided error is an ErrorCollection
func IsErrorCollection(err error) bool {
	_, ok := err.(QueryErrorCollection)
	return ok
}

func AllErrorsFor(collection QueryErrorCollection) []error {
	var errs []error
	for _, qe := range collection.Errors() {
		if qe.Error != nil {
			errs = append(errs, qe.Error)
		}
		if qe.ParseError != nil {
			errs = append(errs, qe.ParseError)
		}
	}
	return errs
}

// WrapError wraps the given error with the given message, usually for adding
// context, but persists the existing type of error.
func WrapError(err error, msg string) error {
	switch e := err.(type) {
	case CommError:
		return e.Wrap(msg)
	case NoDataError:
		return e.Wrap(msg)
	default:
		return fmt.Errorf("%s: %s", msg, err)
	}
}

// CommError describes an error communicating with Prometheus
type CommError struct {
	messages []string
}

// NewCommError creates a new CommError
func NewCommError(messages ...string) CommError {
	return CommError{messages: messages}
}

// CommErrorf creates a new CommError using a string formatter
func CommErrorf(format string, args ...interface{}) CommError {
	return NewCommError(fmt.Sprintf(format, args...))
}

// IsCommError returns true if the given error is a CommError
func IsCommError(err error) bool {
	_, ok := err.(CommError)
	return ok
}

// Error prints the error as a string
func (pce CommError) Error() string {
	return fmt.Sprintf("Prometheus communication error: %s", strings.Join(pce.messages, ": "))
}

// Wrap wraps the error with the given message, but persists the error type.
func (pce CommError) Wrap(message string) CommError {
	pce.messages = append([]string{message}, pce.messages...)
	return pce
}

// NoDataError indicates that no data was returned by Prometheus. This should
// be treated like an EOF error, in that it may be expected.
type NoDataError struct {
	messages []string
}

// NewNoDataError creates a new NoDataError
func NewNoDataError(messages ...string) NoDataError {
	return NoDataError{messages: messages}
}

// IsNoDataError returns true if the given error is a NoDataError
func IsNoDataError(err error) bool {
	_, ok := err.(NoDataError)
	return ok
}

// Error prints the error as a string
func (nde NoDataError) Error() string {
	return fmt.Sprintf("No data error: %s", strings.Join(nde.messages, ": "))
}

// Wrap wraps the error with the given message, but persists the error type.
func (nde NoDataError) Wrap(message string) NoDataError {
	nde.messages = append([]string{message}, nde.messages...)
	return nde
}
