package promutil

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/opencost/opencost/core/pkg/util/json"
)

var invalidLabelCharRE = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// AnyToLabels will create prometheus labels based on the fields of the interface
// passed. Note that this method is quite expensive and should only be used when absolutely
// necessary.
func AnyToLabels(a interface{}) (map[string]string, error) {
	val := reflect.ValueOf(a)
	if val.Kind() == reflect.Map {
		return MapToLabels(a), nil
	}

	b, e := json.Marshal(a)
	if e != nil {
		return nil, e
	}

	var m map[string]interface{}
	e = json.Unmarshal(b, &m)
	if e != nil {
		return nil, e
	}

	return MapToLabels(m), nil
}

// MapToLabels accepts a map type, and will return a new map containing all the nested
// fields separated by _ with string versions of the values.
func MapToLabels(m interface{}) map[string]string {
	val := reflect.ValueOf(m)
	if val.Kind() != reflect.Map {
		return map[string]string{}
	}

	r := make(map[string]string)

	for _, k := range val.MapKeys() {
		key := strings.ToLower(k.String())
		v := val.MapIndex(k).Interface()

		switch v.(type) {
		case uint, uint8, uint16, uint32, uint64, int, int8, int16, int32, int64, string, bool, float32, float64:
			r[key] = fmt.Sprintf("%+v", v)

		default:
			mm := MapToLabels(v)
			for kk, vv := range mm {
				r[fmt.Sprintf("%s_%s", key, kk)] = vv
			}
		}
	}

	return r
}

// LabelNamesFrom accepts a mapping of labels to values and returns the label names.
func LabelNamesFrom(labels map[string]string) []string {
	keys := []string{}
	for key := range labels {
		keys = append(keys, key)
	}
	return keys
}

// Prepends a qualifier string to the keys provided in the m map and returns the new keys and values.
func KubePrependQualifierToLabels(m map[string]string, qualifier string) ([]string, []string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	values := make([]string, 0, len(m))
	for i, k := range keys {
		keys[i] = qualifier + SanitizeLabelName(k)
		values = append(values, m[k])
	}

	return keys, values
}

// Converts kubernetes labels into prometheus labels.
func KubeLabelsToLabels(labels map[string]string) ([]string, []string) {
	return KubePrependQualifierToLabels(labels, "label_")
}

// Converts kubernetes annotations into prometheus labels.
func KubeAnnotationsToLabels(labels map[string]string) ([]string, []string) {
	return KubePrependQualifierToLabels(labels, "annotation_")
}

// Replaces all illegal prometheus label characters with _
func SanitizeLabelName(s string) string {
	return invalidLabelCharRE.ReplaceAllString(s, "_")
}

// SanitizeLabels sanitizes all label names in the given map. This may cause
// collisions, which is intentional as collisions that are not caught prior to
// attempted emission will cause fatal errors. In the case of a collision, the
// last value seen will be set, and all previous values will be overwritten.
func SanitizeLabels(labels map[string]string) map[string]string {
	response := make(map[string]string, len(labels))

	for k, v := range labels {
		response[SanitizeLabelName(k)] = v
	}

	return response
}
