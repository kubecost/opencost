package oracle

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/stretchr/testify/assert"
)

func TestRegionValidation(t *testing.T) {
	for _, r := range oracleRegions() {
		// Use the OCI SDK to validate static regions.
		region := common.StringToRegion(r)
		_, err := region.RealmID()
		assert.NoError(t, err)
	}
}
