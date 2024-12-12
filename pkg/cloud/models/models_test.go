package models

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSetSetCustomPricingField(t *testing.T) {
	defaultValue := "1.0"

	type testCase struct {
		testName   string
		fieldName  string
		fieldValue string
		expValue   string
		expError   error
	}

	testCaseTemplates := []testCase{
		{
			testName:   "valid number for %s",
			fieldName:  "%s",
			fieldValue: "0.04321",
			expValue:   "0.04321",
			expError:   nil,
		},
		{
			testName:   "long number for %s",
			fieldName:  "%s",
			fieldValue: "0.04321234321231234",
			expValue:   "0.04321234321231234",
			expError:   nil,
		},
		{
			testName:   "illegal number for %s",
			fieldName:  "%s",
			fieldValue: "0.123.123",
			expValue:   defaultValue, // assert that the default value is not mutated
			expError:   fmt.Errorf("invalid numeric value for field"),
		},
		{
			testName:   "NaN for %s",
			fieldName:  "%s",
			fieldValue: "NaN",
			expValue:   defaultValue, // assert that the default value is not mutated
			expError:   fmt.Errorf("invalid numeric value for field"),
		},
		{
			testName:   "empty string for %s",
			fieldName:  "%s",
			fieldValue: "",
			expValue:   defaultValue, // assert that the default value is not mutated
			expError:   nil,
		},
	}

	numericFields := []string{
		"CPU",
		"GPU",
		"RAM",
		"SpotCPU",
		"SpotGPU",
		"SpotRAM",
		"Storage",
		"ZoneNetworkEgress",
		"RegionNetworkEgress",
		"InternetNetworkEgress",
	}

	testCases := []testCase{}

	// Build one test case per-template, per-numeric field; this is obscure
	// but it prevents me from having to write the same test for all 10
	// numeric fields...
	for _, field := range numericFields {
		for _, tpl := range testCaseTemplates {
			testCases = append(testCases, testCase{
				testName:   fmt.Sprintf(tpl.testName, field),
				fieldName:  fmt.Sprintf(tpl.fieldName, field),
				fieldValue: tpl.fieldValue,
				expValue:   tpl.expValue,
				expError:   tpl.expError,
			})
		}
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			cp := &CustomPricing{
				CPU:                   defaultValue,
				SpotCPU:               defaultValue,
				RAM:                   defaultValue,
				SpotRAM:               defaultValue,
				GPU:                   defaultValue,
				SpotGPU:               defaultValue,
				Storage:               defaultValue,
				ZoneNetworkEgress:     defaultValue,
				RegionNetworkEgress:   defaultValue,
				InternetNetworkEgress: defaultValue,
			}
			err := SetCustomPricingField(cp, tc.fieldName, tc.fieldValue)
			if err != nil && tc.expError == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if err == nil && tc.expError != nil {
				t.Errorf("did not find expected error: %s", tc.expError)
			}

			structValue := reflect.ValueOf(cp).Elem()
			structFieldValue := structValue.FieldByName(tc.fieldName)
			actValue := structFieldValue.String()
			if actValue != tc.expValue {
				t.Errorf("expected field '%s' to be '%s'; actual value is '%s'", tc.fieldName, tc.expValue, actValue)
			}
		})
	}
}
