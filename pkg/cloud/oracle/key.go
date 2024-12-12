package oracle

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/util"
)

type oracleKey struct {
	gpuCount     int
	gpuType      string
	providerID   string
	instanceType string
	labels       map[string]string
}

func (k *oracleKey) ID() string {
	return k.providerID
}

// Features are the OCI node features: compute, memory, and optionally gpu.
func (k *oracleKey) Features() string {
	arch, ok := util.GetArchType(k.labels)
	if !ok {
		arch = "amd64"
	}
	return fmt.Sprintf("%s,%t,%s", k.instanceType, k.isVirtualNode(), arch)
}

func (k *oracleKey) GPUType() string {
	return k.gpuType
}

func (k *oracleKey) GPUCount() int {
	return k.gpuCount
}

func (k *oracleKey) isVirtualNode() bool {
	_, ok := k.labels[virtualNodeLabel]
	return ok
}

const driverOCI = "oracle.com/oci"
const driverOCIBV = "blockvolume.csi.oraclecloud.com"

// ociStorageDrivers are the known storage drivers for OCI.
var ociStorageDrivers = map[string]bool{
	driverOCI:   true,
	driverOCIBV: true,
}

type oraclePVKey struct {
	storageClass string
	driver       string
	providerID   string
	parameters   map[string]string
}

func (p *oraclePVKey) Features() string {
	if ociStorageDrivers[p.driver] {
		return blockVolumePartNumber
	}
	return ""
}

func (p *oraclePVKey) GetStorageClass() string {
	return p.storageClass
}

func (p *oraclePVKey) ID() string {
	return p.providerID
}
