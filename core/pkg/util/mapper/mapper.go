package mapper

import (
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

//--------------------------------------------------------------------------
//  Contracts
//--------------------------------------------------------------------------

// Getter is an interface that retrieves a string value for a string key and
// can check for the existence of a string key.
type Getter interface {
	Get(key string) string
	Has(key string) bool
}

// Setter is an interface that sets the value of a string key to a string value.
type Setter interface {
	Set(key string, value string) error
}

// Map is an interface that gets and sets the value of string keys to/from
// string values
type Map interface {
	Getter
	Setter
}

// PrimitiveMapReader is an implementation contract for an object capable
// of reading primitive values from a util.Map
type PrimitiveMapReader interface {
	// Has checks if the map contains the given key.
	Has(key string) bool

	// Get parses an string from the map key parameter. If the value
	// is empty, the defaultValue parameter is returned.
	Get(key string, defaultValue string) string

	// GetInt parses an int from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetInt(key string, defaultValue int) int

	// GetInt8 parses an int8 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetInt8(key string, defaultValue int8) int8

	// GetInt16 parses an int16 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetInt16(key string, defaultValue int16) int16

	// GetInt32 parses an int32 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetInt32(key string, defaultValue int32) int32

	// GetInt64 parses an int64 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetInt64(key string, defaultValue int64) int64

	// GetUInt parses a uint from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetUInt(key string, defaultValue uint) uint

	// GetUInt8 parses a uint8 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetUInt8(key string, defaultValue uint8) uint8

	// GetUInt16 parses a uint16 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetUInt16(key string, defaultValue uint16) uint16

	// GetUInt32 parses a uint32 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetUInt32(key string, defaultValue uint32) uint32

	// GetUInt64 parses a uint64 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetUInt64(key string, defaultValue uint64) uint64

	// GetFloat32 parses a float32 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetFloat32(key string, defaultValue float32) float32

	// GetFloat64 parses a float64 from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetFloat64(key string, defaultValue float64) float64

	// GetBool parses a bool from the map key parameter. If the value
	// is empty or fails to parse, the defaultValue parameter is returned.
	GetBool(key string, defaultValue bool) bool

	// GetDuration parses a time.Duration from the map key parameter. If the
	// value is empty to fails to parse, the defaultValue is returned.
	GetDuration(key string, defaultValue time.Duration) time.Duration

	// GetList returns a string list which contains the value set by key split using the
	// provided delimiter with each entry trimmed of space. If the value doesn't exist,
	// nil is returned
	GetList(key string, delimiter string) []string
}

// PrimitiveMapWriter is an implementation contract for an object capable
// of write primitive values to a util.Map
type PrimitiveMapWriter interface {
	// Set sets the map for the key provided using the value provided.
	Set(key string, value string) error

	// SetInt sets the map to a string formatted int value
	SetInt(key string, value int) error

	// SetInt8 sets the map to a string formatted int8 value.
	SetInt8(key string, value int8) error

	// SetInt16 sets the map to a string formatted int16 value.
	SetInt16(key string, value int16) error

	// SetInt32 sets the map to a string formatted int32 value.
	SetInt32(key string, value int32) error

	// SetInt64 sets the map to a string formatted int64 value.
	SetInt64(key string, value int64) error

	// SetUInt sets the map to a string formatted uint value
	SetUInt(key string, value uint) error

	// SetUInt8 sets the map to a string formatted uint8 value
	SetUInt8(key string, value uint8) error

	// SetUInt16 sets the map to a string formatted uint16 value
	SetUInt16(key string, value uint16) error

	// SetUInt32 sets the map to a string formatted uint32 value
	SetUInt32(key string, value uint32) error

	// SetUInt64 sets the map to a string formatted uint64 value
	SetUInt64(key string, value uint64) error

	// SetBool sets the map to a string formatted bool value.
	SetBool(key string, value bool) error

	// SetDuration sets the map to a string formatted time.Duration value
	SetDuration(key string, duration time.Duration) error

	// SetList sets the map's value at key to a string consistent of each value in the list separated
	// by the provided delimiter.
	SetList(key string, values []string, delimiter string) error
}

// PrimitiveMap is capable of reading and writing primitive values
// to/from a util.Map
type PrimitiveMap interface {
	PrimitiveMapReader
	PrimitiveMapWriter
}

//--------------------------------------------------------------------------
//  Go Map Implementation
//--------------------------------------------------------------------------

// GoMap is an implementation of mapper.Map for map[string]string
type GoMap struct {
	m map[string]string
}

// Has implements mapper.Haser
func (gm *GoMap) Has(key string) bool {
	_, ok := gm.m[key]
	return ok
}

// Get implements mapper.Getter
func (gm *GoMap) Get(key string) string {
	return gm.m[key]
}

// Set implements mapper.Setter
func (gm *GoMap) Set(key, value string) error {
	gm.m[key] = value
	return nil
}

// NewGoMap creates a Map from a map[string]string. It copies
// data out of the argument.
func NewGoMap(m map[string]string) Map {
	copied := map[string]string{}
	for k, v := range m {
		copied[k] = v
	}
	return &GoMap{m: copied}
}

// NewMap creates a new mapper.Map implementation
func NewMap() Map {
	return &GoMap{
		m: make(map[string]string),
	}
}

//--------------------------------------------------------------------------
//  PrimitiveMap Implementation
//--------------------------------------------------------------------------

// readOnlyMapper provides Get methods for most primitive go types
type readOnlyMapper struct {
	getter Getter
}

// writeOnlyMapper provides Set methods for most primitive go types
type writeOnlyMapper struct {
	setter Setter
}

// mapper provides Get and Set methods for most primitive go types
type mapper struct {
	*readOnlyMapper
	*writeOnlyMapper
}

// NewReadOnlyMapper creates a new implementation of a PrimitiveMapReader
func NewReadOnlyMapper(getter Getter) PrimitiveMapReader {
	return &readOnlyMapper{getter}
}

// NewWriteOnlyMapper creates a new implementation of a PrimitiveMapWriter
func NewWriteOnlyMapper(setter Setter) PrimitiveMapWriter {
	return &writeOnlyMapper{setter}
}

// NewMapper creates a new implementation of a PrimitiveMap
func NewMapper(m Map) PrimitiveMap {
	return &mapper{
		readOnlyMapper:  &readOnlyMapper{m},
		writeOnlyMapper: &writeOnlyMapper{m},
	}
}

// NewCompositionMapper creates a new implementation of a PrimitiveMap composed of a
// custom Getter implementation and Setter implementation
func NewCompositionMapper(getter Getter, setter Setter) PrimitiveMap {
	return &mapper{
		readOnlyMapper:  &readOnlyMapper{getter},
		writeOnlyMapper: &writeOnlyMapper{setter},
	}
}

func (rom *readOnlyMapper) Has(key string) bool {
	return rom.getter.Has(key)
}

// Get parses an string from the read-only mapper key parameter. If the value
// is empty, the defaultValue parameter is returned.
func (rom *readOnlyMapper) Get(key string, defaultValue string) string {
	r := rom.getter.Get(key)
	if r == "" {
		return defaultValue
	}

	return r
}

// GetInt parses an int from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetInt(key string, defaultValue int) int {
	r := rom.getter.Get(key)
	i, err := strconv.Atoi(r)
	if err != nil {
		return defaultValue
	}

	return i
}

// GetInt8 parses an int8 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetInt8(key string, defaultValue int8) int8 {
	r := rom.getter.Get(key)
	i, err := strconv.ParseInt(r, 10, 8)
	if err != nil {
		return defaultValue
	}

	return int8(i)
}

// GetInt16 parses an int16 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetInt16(key string, defaultValue int16) int16 {
	r := rom.getter.Get(key)
	i, err := strconv.ParseInt(r, 10, 16)
	if err != nil {
		return defaultValue
	}

	return int16(i)
}

// GetInt32 parses an int32 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetInt32(key string, defaultValue int32) int32 {
	r := rom.getter.Get(key)
	i, err := strconv.ParseInt(r, 10, 32)
	if err != nil {
		return defaultValue
	}

	return int32(i)
}

// GetInt64 parses an int64 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetInt64(key string, defaultValue int64) int64 {
	r := rom.getter.Get(key)
	i, err := strconv.ParseInt(r, 10, 64)
	if err != nil {
		return defaultValue
	}

	return i
}

// GetUInt parses a uint from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetUInt(key string, defaultValue uint) uint {
	r := rom.getter.Get(key)
	i, err := strconv.ParseUint(r, 10, 32)
	if err != nil {
		return defaultValue
	}

	return uint(i)
}

// GetUInt8 parses a uint8 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetUInt8(key string, defaultValue uint8) uint8 {
	r := rom.getter.Get(key)
	i, err := strconv.ParseUint(r, 10, 8)
	if err != nil {
		return defaultValue
	}

	return uint8(i)
}

// GetUInt16 parses a uint16 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetUInt16(key string, defaultValue uint16) uint16 {
	r := rom.getter.Get(key)
	i, err := strconv.ParseUint(r, 10, 16)
	if err != nil {
		return defaultValue
	}

	return uint16(i)
}

// GetUInt32 parses a uint32 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetUInt32(key string, defaultValue uint32) uint32 {
	r := rom.getter.Get(key)
	i, err := strconv.ParseUint(r, 10, 32)
	if err != nil {
		return defaultValue
	}

	return uint32(i)
}

// GetUInt64 parses a uint64 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetUInt64(key string, defaultValue uint64) uint64 {
	r := rom.getter.Get(key)
	i, err := strconv.ParseUint(r, 10, 64)
	if err != nil {
		return defaultValue
	}

	return uint64(i)
}

// GetFloat32 parses a float32 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetFloat32(key string, defaultValue float32) float32 {
	r := rom.getter.Get(key)
	f, err := strconv.ParseFloat(r, 32)
	if err != nil {
		return defaultValue
	}

	return float32(f)
}

// GetFloat64 parses a float64 from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetFloat64(key string, defaultValue float64) float64 {
	r := rom.getter.Get(key)
	f, err := strconv.ParseFloat(r, 64)
	if err != nil {
		return defaultValue
	}

	return f
}

// GetBool parses a bool from the read-only mapper key parameter. If the value
// is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetBool(key string, defaultValue bool) bool {
	r := rom.getter.Get(key)
	b, err := strconv.ParseBool(r)
	if err != nil {
		return defaultValue
	}

	return b
}

// GetDuration parses a time.Duration from the read-only mapper key parameter.
// If the value is empty or fails to parse, the defaultValue parameter is returned.
func (rom *readOnlyMapper) GetDuration(key string, defaultValue time.Duration) time.Duration {
	r := rom.getter.Get(key)

	d, err := timeutil.ParseDuration(r)
	if err != nil {
		return defaultValue
	}

	return d
}

// GetList returns a string list which contains the value set by key split using the
// provided delimiter with each entry trimmed of space. If the value doesn't exist,
// nil is returned
func (rom *readOnlyMapper) GetList(key string, delimiter string) []string {
	value := rom.Get(key, "")
	if value == "" {
		return nil
	}

	split := strings.Split(value, delimiter)

	// reuse slice created for split
	result := split[:0]
	for _, v := range split {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// Set sets the map for the key provided using the value provided.
func (wom *writeOnlyMapper) Set(key string, value string) error {
	return wom.setter.Set(key, value)
}

// SetInt sets the map to a string formatted int value
func (wom *writeOnlyMapper) SetInt(key string, value int) error {
	return wom.setter.Set(key, strconv.Itoa(value))
}

// SetInt8 sets the map to a string formatted int8 value.
func (wom *writeOnlyMapper) SetInt8(key string, value int8) error {
	return wom.setter.Set(key, strconv.FormatInt(int64(value), 10))
}

// SetInt16 sets the map to a string formatted int16 value.
func (wom *writeOnlyMapper) SetInt16(key string, value int16) error {
	return wom.setter.Set(key, strconv.FormatInt(int64(value), 10))
}

// SetInt32 sets the map to a string formatted int32 value.
func (wom *writeOnlyMapper) SetInt32(key string, value int32) error {
	return wom.setter.Set(key, strconv.FormatInt(int64(value), 10))
}

// SetInt64 sets the map to a string formatted int64 value.
func (wom *writeOnlyMapper) SetInt64(key string, value int64) error {
	return wom.setter.Set(key, strconv.FormatInt(value, 10))
}

// SetUInt sets the map to a string formatted uint value
func (wom *writeOnlyMapper) SetUInt(key string, value uint) error {
	return wom.setter.Set(key, strconv.FormatUint(uint64(value), 10))
}

// SetUInt8 sets the map to a string formatted uint8 value
func (wom *writeOnlyMapper) SetUInt8(key string, value uint8) error {
	return wom.setter.Set(key, strconv.FormatUint(uint64(value), 10))
}

// SetUInt16 sets the map to a string formatted uint16 value
func (wom *writeOnlyMapper) SetUInt16(key string, value uint16) error {
	return wom.setter.Set(key, strconv.FormatUint(uint64(value), 10))
}

// SetUInt32 sets the map to a string formatted uint32 value
func (wom *writeOnlyMapper) SetUInt32(key string, value uint32) error {
	return wom.setter.Set(key, strconv.FormatUint(uint64(value), 10))
}

// SetUInt64 sets the map to a string formatted uint64 value
func (wom *writeOnlyMapper) SetUInt64(key string, value uint64) error {
	return wom.setter.Set(key, strconv.FormatUint(value, 10))
}

// SetBool sets the map to a string formatted bool value.
func (wom *writeOnlyMapper) SetBool(key string, value bool) error {
	return wom.setter.Set(key, strconv.FormatBool(value))
}

// SetDuration sets the map to a string formatted bool value.
func (wom *writeOnlyMapper) SetDuration(key string, value time.Duration) error {
	return wom.setter.Set(key, timeutil.DurationString(value))
}

// SetList sets the map's value at key to a string consistent of each value in the list separated
// by the provided delimiter.
func (wom *writeOnlyMapper) SetList(key string, values []string, delimiter string) error {
	return wom.setter.Set(key, strings.Join(values, delimiter))
}
