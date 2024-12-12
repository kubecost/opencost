package gcp

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestBigQueryIntegration_GetCloudCost(t *testing.T) {
	bigQueryConfigPath := os.Getenv("BIGQUERY_CONFIGURATION")
	if bigQueryConfigPath == "" {
		t.Skip("skipping integration test, set environment variable BIGQUERY_CONFIGURATION\"")
	}
	bigQueryConfigBin, err := os.ReadFile(bigQueryConfigPath)
	if err != nil {
		t.Fatalf("failed to read config file: %s", err.Error())
	}
	var bigQueryConfig BigQueryConfiguration
	err = json.Unmarshal(bigQueryConfigBin, &bigQueryConfig)
	if err != nil {
		t.Fatalf("failed to unmarshal config from JSON: %s", err.Error())
	}

	today := opencost.RoundBack(time.Now().UTC(), timeutil.Day)

	testCases := map[string]struct {
		integration *BigQueryIntegration
		start       time.Time
		end         time.Time
		expected    bool
	}{

		"last week window": {
			integration: &BigQueryIntegration{
				BigQueryQuerier: BigQueryQuerier{
					BigQueryConfiguration: bigQueryConfig,
				},
			},
			end:      today.Add(-7 * timeutil.Day),
			start:    today.Add(-8 * timeutil.Day),
			expected: false,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := testCase.integration.GetCloudCost(testCase.start, testCase.end)
			if err != nil {
				t.Errorf("Other error during testing %s", err)
			} else if actual.IsEmpty() != testCase.expected {
				t.Errorf("Incorrect result, actual emptiness: %t, expected: %t", actual.IsEmpty(), testCase.expected)
			}
		})
	}
}
