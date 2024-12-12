package oracle

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRCSForKey(t *testing.T) {
	rcs, server := testSetupRateCardStore(t)
	defer server.Close()

	testCases := map[string]struct {
		cost string
		arm  bool
	}{
		"VM.DenseIO.E4.Flex": {
			"0.014061",
			false,
		},
		"BM.GPU3.8": {
			"2.950000",
			false,
		},
		"Pod.Standard.A1.Flex": {
			"0.0115",
			true,
		},
		"VM.Standard.E4.Flex": {
			"0.014000",
			false,
		},
		"VM.Standard.E3.Flex": {
			"0.014000",
			false,
		},
		"unknown-shape": {
			"0.600000",
			false,
		},
	}

	for instanceType, testCase := range testCases {
		t.Run(instanceType, func(t *testing.T) {
			key := &oracleKey{
				instanceType: instanceType,
				labels:       make(map[string]string),
			}
			if testCase.arm {
				key.labels["kubernetes.io/arch"] = "arm64"
			}
			node, _, err := rcs.ForKey(key, DefaultPricing{
				OCPU:   "0.2",
				Memory: "0.1",
				GPU:    "0.3",
			})
			assert.NoError(t, err)
			assertFloatStrings(t, testCase.cost, node.Cost, 0.001)
		})
	}
}

func TestRCSForPVK(t *testing.T) {
	rcs, server := testSetupRateCardStore(t)
	defer server.Close()

	var testCases = map[string]struct {
		cost string
	}{
		driverOCIBV: {
			"0.000034",
		},
		driverOCI: {
			"0.000034",
		},
		"unknown": {
			"0.25",
		},
	}

	for driver, testCase := range testCases {
		t.Run(driver, func(t *testing.T) {
			pvk := &oraclePVKey{
				driver: driver,
			}
			pv, err := rcs.ForPVK(pvk, DefaultPricing{
				Storage: "0.25",
			})
			assert.NoError(t, err)
			assertFloatStrings(t, testCase.cost, pv.Cost, 0.00001)
		})
	}
}

func TestRCSEgressForRegion(t *testing.T) {
	rcs, server := testSetupRateCardStore(t)
	defer server.Close()

	var testCases = map[string]struct {
		cost float64
	}{
		"ap-mumbai-1": {
			0.025,
		},
		"sa-saopaulo-1": {
			0.025,
		},
		"me-dubai-1": {
			0.05,
		},
		"af-johannesburg-1": {
			0.05,
		},
		"il-jerusalem-1": {
			0.05,
		},
		"eu-madrid-1": {
			0.0085,
		},
		"uk-cardiff-1": {
			0.0085,
		},
		"mx-monterrey-1": {
			0.0085,
		},
		"us-chicago-1": {
			0.0085,
		},
		"ca-montreal-1": {
			0.0085,
		},
		"unknown": {
			0.000123,
		},
	}
	for region, testCase := range testCases {
		net, err := rcs.ForEgressRegion(region, DefaultPricing{
			Egress: "0.000123",
		})
		assert.NoError(t, err)
		assert.InDelta(t, float64(0), net.ZoneNetworkEgressCost, 0.1)
		assert.InDelta(t, testCase.cost, net.RegionNetworkEgressCost, 0.001)
		assert.InDelta(t, testCase.cost, net.InternetNetworkEgressCost, 0.001)
	}
}

func testSetupRateCardStore(t *testing.T) (*RateCardStore, *httptest.Server) {
	pricesUSDBytes, err := os.ReadFile("test/prices_usd.json")
	assert.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(pricesUSDBytes)
	}))

	rcs := NewRateCardStore(server.URL, currencyCodeUSD)
	store, err := rcs.Refresh()
	assert.NoError(t, err)
	assert.True(t, len(store) > 0)
	return rcs, server
}

func assertFloatStrings(t *testing.T, s1, s2 string, delta float64) {
	f1, err := strconv.ParseFloat(s1, 64)
	assert.NoError(t, err)
	f2, err := strconv.ParseFloat(s2, 64)
	assert.NoError(t, err)
	assert.InDelta(t, f1, f2, delta)
}
