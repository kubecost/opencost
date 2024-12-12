package env

import (
	"os"
	"time"

	"github.com/opencost/opencost/core/pkg/util/mapper"
)

//--------------------------------------------------------------------------
//  EnvVar mapper.Map Implementation
//--------------------------------------------------------------------------

// envMap contains Getter and Setter implementations for environment variables
type envMap struct{}

func (em *envMap) Has(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}

// Get returns the value for the provided environment variable
func (em *envMap) Get(key string) string {
	return os.Getenv(key)
}

// Set sets the value for the provided key and returns true if successful. Otherwise,
// false is returned.
func (em *envMap) Set(key string, value string) error {
	return os.Setenv(key, value)
}

// This PrimitiveMapper implementation leverages os.Getenv() and os.Setenv() to get/set
// primitive go values as environment variables.
var envMapper mapper.PrimitiveMap = mapper.NewMapper(&envMap{})

//--------------------------------------------------------------------------
//  Package Funcs
//--------------------------------------------------------------------------

// Get parses an string from the environment variable key parameter. If the environment
// variable is empty, the defaultValue parameter is returned.
func Get(key string, defaultValue string) string {
	return envMapper.Get(key, defaultValue)
}

// GetInt parses an int from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt(key string, defaultValue int) int {
	return envMapper.GetInt(key, defaultValue)
}

// GetInt8 parses an int8 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt8(key string, defaultValue int8) int8 {
	return envMapper.GetInt8(key, defaultValue)
}

// GetInt16 parses an int16 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt16(key string, defaultValue int16) int16 {
	return envMapper.GetInt16(key, defaultValue)
}

// GetInt32 parses an int32 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt32(key string, defaultValue int32) int32 {
	return envMapper.GetInt32(key, defaultValue)
}

// GetInt64 parses an int64 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt64(key string, defaultValue int64) int64 {
	return envMapper.GetInt64(key, defaultValue)
}

// GetUInt parses a uint from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt(key string, defaultValue uint) uint {
	return envMapper.GetUInt(key, defaultValue)
}

// GetUInt8 parses a uint8 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt8(key string, defaultValue uint8) uint8 {
	return envMapper.GetUInt8(key, defaultValue)
}

// GetUInt16 parses a uint16 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt16(key string, defaultValue uint16) uint16 {
	return envMapper.GetUInt16(key, defaultValue)
}

// GetUInt32 parses a uint32 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt32(key string, defaultValue uint32) uint32 {
	return envMapper.GetUInt32(key, defaultValue)
}

// GetUInt64 parses a uint64 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt64(key string, defaultValue uint64) uint64 {
	return envMapper.GetUInt64(key, defaultValue)
}

// GetFloat32 parses a float32 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetFloat32(key string, defaultValue float32) float32 {
	return envMapper.GetFloat32(key, defaultValue)
}

// GetFloat64 parses a float64 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetFloat64(key string, defaultValue float64) float64 {
	return envMapper.GetFloat64(key, defaultValue)
}

// GetBool parses a bool from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetBool(key string, defaultValue bool) bool {
	return envMapper.GetBool(key, defaultValue)
}

// GetDuration parses a time.Duration from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetDuration(key string, defaultValue time.Duration) time.Duration {
	return envMapper.GetDuration(key, defaultValue)
}

// GetList parses a []string from the environment variable key parameter.  If the environment
// // variable is empty or fails to parse, nil is returned.
func GetList(key, delimiter string) []string {
	return envMapper.GetList(key, delimiter)
}

// Set sets the environment variable for the key provided using the value provided.
func Set(key string, value string) error {
	return envMapper.Set(key, value)
}

// SetInt sets the environment variable to a string formatted int value
func SetInt(key string, value int) error {
	return envMapper.SetInt(key, value)
}

// SetInt8 sets the environment variable to a string formatted int8 value.
func SetInt8(key string, value int8) error {
	return envMapper.SetInt8(key, value)
}

// SetInt16 sets the environment variable to a string formatted int16 value.
func SetInt16(key string, value int16) error {
	return envMapper.SetInt16(key, value)
}

// SetInt32 sets the environment variable to a string formatted int32 value.
func SetInt32(key string, value int32) error {
	return envMapper.SetInt32(key, value)
}

// SetInt64 sets the environment variable to a string formatted int64 value.
func SetInt64(key string, value int64) error {
	return envMapper.SetInt64(key, value)
}

// SetUInt sets the environment variable to a string formatted uint value
func SetUInt(key string, value uint) error {
	return envMapper.SetUInt(key, value)
}

// SetUInt8 sets the environment variable to a string formatted uint8 value
func SetUInt8(key string, value uint8) error {
	return envMapper.SetUInt8(key, value)
}

// SetUInt16 sets the environment variable to a string formatted uint16 value
func SetUInt16(key string, value uint16) error {
	return envMapper.SetUInt16(key, value)
}

// SetUInt32 sets the environment variable to a string formatted uint32 value
func SetUInt32(key string, value uint32) error {
	return envMapper.SetUInt32(key, value)
}

// SetUInt64 sets the environment variable to a string formatted uint64 value
func SetUInt64(key string, value uint64) error {
	return envMapper.SetUInt64(key, value)
}

// SetBool sets the environment variable to a string formatted bool value.
func SetBool(key string, value bool) error {
	return envMapper.SetBool(key, value)
}

// SetDuration sets the environment variable to a string formatted time.Duration
func SetDuration(key string, value time.Duration) error {
	return envMapper.SetDuration(key, value)
}
