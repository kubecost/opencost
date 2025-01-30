package linode

import (
	"fmt"
	"io"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/env"
)

func (l *Linode) GetConfig() (*models.CustomPricing, error) {
	c, err := l.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}

	if c.Discount == "" {
		c.Discount = "0%"
	}

	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}

	if c.CurrencyCode == "" {
		c.CurrencyCode = "USD"
	}

	return c, nil
}

func (l *Linode) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return l.Config.UpdateFromMap(a)
}

func (l *Linode) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	defer l.DownloadPricingData()

	return l.Config.Update(func(c *models.CustomPricing) error {
		a := make(map[string]interface{})
		err := json.NewDecoder(r).Decode(&a)
		if err != nil {
			return err
		}
		for k, v := range a {
			kUpper := utils.ToTitle.String(k) // Just so we consistently supply / receive the same values, uppercase the first letter.
			vstr, ok := v.(string)
			if ok {
				err := models.SetCustomPricingField(c, kUpper, vstr)
				if err != nil {
					return fmt.Errorf("error setting custom pricing field: %w", err)
				}
			} else {
				return fmt.Errorf("type error while updating config for %s", kUpper)
			}
		}

		if env.IsRemoteEnabled() {
			err := utils.UpdateClusterMeta(env.GetClusterID(), c.ClusterName)
			if err != nil {
				return err
			}
		}

		return nil
	})
}
