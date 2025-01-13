package linode

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
)

type linodeKey struct {
	Labels map[string]string
	Name   string
}

func (k *linodeKey) Features() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	region, _ := util.GetRegion(k.Labels)

	return fmt.Sprintf("%s,%s", region, instanceType)
}

func (k *linodeKey) GPUCount() int {
	return 0
}

func (k *linodeKey) GPUType() string {
	return ""
}

func (k *linodeKey) ID() string {
	return k.Name
}

type linodePVKey struct {
	Labels                 map[string]string
	StorageClassName       string
	StorageClassParameters map[string]string
	Name                   string
	Region                 string
}

func (k *linodePVKey) ID() string {
	return ""
}

func (k *linodePVKey) GetStorageClass() string {
	return k.StorageClassName
}

func (k *linodePVKey) Features() string {
	return k.Region
}

func (l *Linode) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	return &linodeKey{
		Labels: labels,
		Name:   n.Name,
	}
}

func (l *Linode) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, _ string) models.PVKey {
	return &linodePVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		Name:                   pv.Name,
		Region:                 l.ClusterRegion,
	}
}
