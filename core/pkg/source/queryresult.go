package source

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util"
)

// QueryResultsChan is a channel of query results
type QueryResultsChan chan *QueryResults

// Await returns query results, blocking until they are made available, and
// deferring the closure of the underlying channel
func (qrc QueryResultsChan) Await() ([]*QueryResult, error) {
	defer close(qrc)

	results := <-qrc
	if results.Error != nil {
		return nil, results.Error
	}

	return results.Results, nil
}

type ResultKeys struct {
	ClusterKey      string
	NamespaceKey    string
	NodeKey         string
	InstanceKey     string
	InstanceTypeKey string
	ContainerKey    string
	PodKey          string
	ProviderIDKey   string
	DeviceKey       string
}

func DefaultResultKeys() *ResultKeys {
	return &ResultKeys{
		ClusterKey:      "cluster_id",
		NamespaceKey:    "namespace",
		NodeKey:         "node",
		InstanceKey:     "instance",
		InstanceTypeKey: "instance_type",
		ContainerKey:    "container",
		PodKey:          "pod",
		ProviderIDKey:   "provider_id",
		DeviceKey:       "device",
	}
}

func ClusterKeyWithDefaults(clusterKey string) *ResultKeys {
	keys := DefaultResultKeys()
	keys.ClusterKey = clusterKey
	return keys
}

// QueryResults contains all of the query results and the source query string.
type QueryResults struct {
	Query   string
	Error   error
	Results []*QueryResult
}

func NewQueryResults(query string) *QueryResults {
	return &QueryResults{
		Query: query,
	}
}

// QueryResult contains a single result from a prometheus query. It's common
// to refer to query results as a slice of QueryResult
type QueryResult struct {
	Metric map[string]interface{} `json:"metric"`
	Values []*util.Vector         `json:"values"`

	keys *ResultKeys
}

func NewQueryResult(metrics map[string]any, values []*util.Vector, keys *ResultKeys) *QueryResult {
	if keys == nil {
		keys = DefaultResultKeys()
	}

	return &QueryResult{
		Metric: metrics,
		Values: values,
		keys:   keys,
	}
}

func (qr *QueryResult) GetCluster() (string, error) {
	return qr.GetString(qr.keys.ClusterKey)
}

func (qr *QueryResult) GetNamespace() (string, error) {
	return qr.GetString(qr.keys.NamespaceKey)
}

func (qr *QueryResult) GetNode() (string, error) {
	return qr.GetString(qr.keys.NodeKey)
}

func (qr *QueryResult) GetInstance() (string, error) {
	return qr.GetString(qr.keys.InstanceKey)
}

func (qr *QueryResult) GetInstanceType() (string, error) {
	return qr.GetString(qr.keys.InstanceTypeKey)
}

func (qr *QueryResult) GetContainer() (string, error) {
	value, err := qr.GetString(qr.keys.ContainerKey)
	if value == "" || err != nil {
		alternate, e := qr.GetString(qr.keys.ContainerKey + "_name")
		if alternate == "" || e != nil {
			return "", fmt.Errorf("'%s' and '%s' fields do not exist in data result vector", qr.keys.ContainerKey, qr.keys.ContainerKey+"_name")
		}
		return alternate, nil
	}
	return value, nil
}

func (qr *QueryResult) GetPod() (string, error) {
	value, err := qr.GetString(qr.keys.PodKey)
	if value == "" || err != nil {
		alternate, e := qr.GetString(qr.keys.PodKey + "_name")
		if alternate == "" || e != nil {
			return "", fmt.Errorf("'%s' and '%s' fields do not exist in data result vector", qr.keys.PodKey, qr.keys.PodKey+"_name")
		}
		return alternate, nil
	}
	return value, nil
}

func (qr *QueryResult) GetProviderID() (string, error) {
	return qr.GetString(qr.keys.ProviderIDKey)
}

func (qr *QueryResult) GetDevice() (string, error) {
	return qr.GetString(qr.keys.DeviceKey)
}

// GetString returns the requested field, or an error if it does not exist
func (qr *QueryResult) GetString(field string) (string, error) {
	f, ok := qr.Metric[field]
	if !ok {
		return "", fmt.Errorf("'%s' field does not exist in data result vector", field)
	}

	strField, ok := f.(string)
	if !ok {
		return "", fmt.Errorf("'%s' field is improperly formatted and cannot be converted to string", field)
	}

	return strField, nil
}

// GetStrings returns the requested fields, or an error if it does not exist
func (qr *QueryResult) GetStrings(fields ...string) (map[string]string, error) {
	values := map[string]string{}

	for _, field := range fields {
		f, ok := qr.Metric[field]
		if !ok {
			return nil, fmt.Errorf("'%s' field does not exist in data result vector", field)
		}

		value, ok := f.(string)
		if !ok {
			return nil, fmt.Errorf("'%s' field is improperly formatted and cannot be converted to string", field)
		}

		values[field] = value
	}

	return values, nil
}

// GetLabels returns all labels and their values from the query result
func (qr *QueryResult) GetLabels() map[string]string {
	result := make(map[string]string)

	// Find All keys with prefix label_, remove prefix, add to labels
	for k, v := range qr.Metric {
		if !strings.HasPrefix(k, "label_") {
			continue
		}

		label := strings.TrimPrefix(k, "label_")
		value, ok := v.(string)
		if !ok {
			log.Warnf("Failed to parse label value for label: '%s'", label)
			continue
		}

		result[label] = value
	}

	return result
}

// GetAnnotations returns all annotations and their values from the query result
func (qr *QueryResult) GetAnnotations() map[string]string {
	result := make(map[string]string)

	// Find All keys with prefix annotation_, remove prefix, add to annotations
	for k, v := range qr.Metric {
		if !strings.HasPrefix(k, "annotation_") {
			continue
		}

		annotations := strings.TrimPrefix(k, "annotation_")
		value, ok := v.(string)
		if !ok {
			log.Warnf("Failed to parse label value for label: '%s'", annotations)
			continue
		}

		result[annotations] = value
	}

	return result
}
