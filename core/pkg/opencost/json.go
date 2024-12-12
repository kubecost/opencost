package opencost

import (
	"bytes"
	"fmt"
	"math"

	"github.com/opencost/opencost/core/pkg/util/json"
)

// TODO move everything below to a separate package

func jsonEncodeFloat64(buffer *bytes.Buffer, name string, val float64, comma string) {
	var encoding string
	if math.IsNaN(val) || math.IsInf(val, 0) {
		encoding = fmt.Sprintf("\"%s\":null%s", name, comma)
	} else {
		encoding = fmt.Sprintf("\"%s\":%f%s", name, val, comma)
	}

	buffer.WriteString(encoding)
}

func jsonEncodeString(buffer *bytes.Buffer, name, val, comma string) {
	buffer.WriteString(fmt.Sprintf("\"%s\":\"%s\"%s", name, val, comma))
}

func jsonEncode(buffer *bytes.Buffer, name string, obj interface{}, comma string) {
	buffer.WriteString(fmt.Sprintf("\"%s\":", name))
	if bytes, err := json.Marshal(obj); err != nil {
		buffer.WriteString("null")
	} else {
		buffer.Write(bytes)
	}
	buffer.WriteString(comma)
}
