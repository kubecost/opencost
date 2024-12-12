package oracle

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestUsageAPIIntegration_GetCloudCost(t *testing.T) {
	usageApiConfigPath := os.Getenv("USAGEAPI_CONFIGURATION")
	if usageApiConfigPath == "" {
		t.Skip("skipping integration test, set environment variable USAGEAPI_CONFIGURATION")
	}
	usageApiConfigBin, err := os.ReadFile(usageApiConfigPath)
	if err != nil {
		t.Fatalf("failed to read config file: %s", err.Error())
	}
	var usageApiConfig UsageApiConfiguration
	err = json.Unmarshal(usageApiConfigBin, &usageApiConfig)
	if err != nil {
		t.Fatalf("failed to unmarshal config from JSON: %s", err.Error())
	}
	testCases := map[string]struct {
		integration *UsageApiIntegration
		start       time.Time
		end         time.Time
		expected    bool
	}{
		// No CUR data is expected within 2 days of now
		"too_recent_window": {
			integration: &UsageApiIntegration{
				UsageApiConfiguration: usageApiConfig,
			},
			end:      time.Now(),
			start:    time.Now().Add(-timeutil.Day),
			expected: true,
		},
		// CUR data should be available
		"last week window": {
			integration: &UsageApiIntegration{
				UsageApiConfiguration: usageApiConfig,
			},
			end:      time.Now().Add(-7 * timeutil.Day),
			start:    time.Now().Add(-8 * timeutil.Day),
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
