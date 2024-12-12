package json

import (
	"encoding/json"

	gojson "github.com/goccy/go-json"
	jsoniter "github.com/json-iterator/go"
)

// Marshal leverages the go-json library for Marshalling JSON. This avoids a recursive
// loop that occurs on marshal errors in json-iterator.
var Marshal = gojson.Marshal

// Unmarshal leverages the json-iterator library for Unmarshalling JSON.
var Unmarshal = jsoniter.Unmarshal

var NewEncoder = json.NewEncoder
var NewDecoder = json.NewDecoder

type Marshaler = json.Marshaler
type Unmarshaler = json.Unmarshaler

type RawMessage = json.RawMessage
