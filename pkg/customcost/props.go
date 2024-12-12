package customcost

import (
	"fmt"
	"strings"
)

type CustomCostProperty string

const (
	CustomCostZoneProp           CustomCostProperty = "zone"
	CustomCostAccountNameProp                       = "accountName"
	CustomCostChargeCategoryProp                    = "chargeCategory"
	CustomCostDescriptionProp                       = "description"
	CustomCostResourceNameProp                      = "resourceName"
	CustomCostResourceTypeProp                      = "resourceType"
	CustomCostProviderIdProp                        = "providerId"
	CustomCostUsageUnitProp                         = "usageUnit"
	CustomCostDomainProp                            = "domain"
	CustomCostCostSourceProp                        = "costSource"
)

func ParseCustomCostProperties(props []string) ([]CustomCostProperty, error) {
	var properties []CustomCostProperty
	added := make(map[CustomCostProperty]struct{})

	for _, prop := range props {
		property, err := ParseCustomCostProperty(prop)
		if err != nil {
			return nil, fmt.Errorf("failed to parse property: %w", err)
		}

		if _, ok := added[property]; !ok {
			added[property] = struct{}{}
			properties = append(properties, property)
		}
	}

	return properties, nil
}

func ParseCustomCostProperty(text string) (CustomCostProperty, error) {
	switch strings.TrimSpace(strings.ToLower(text)) {
	case strings.TrimSpace(strings.ToLower(string(CustomCostZoneProp))):
		return CustomCostZoneProp, nil
	case strings.TrimSpace(strings.ToLower(CustomCostAccountNameProp)):
		return CustomCostAccountNameProp, nil
	case strings.TrimSpace(strings.ToLower(CustomCostChargeCategoryProp)):
		return CustomCostChargeCategoryProp, nil
	case strings.TrimSpace(strings.ToLower(CustomCostDescriptionProp)):
		return CustomCostDescriptionProp, nil
	case strings.TrimSpace(strings.ToLower(CustomCostResourceNameProp)):
		return CustomCostResourceNameProp, nil
	case strings.TrimSpace(strings.ToLower(CustomCostResourceTypeProp)):
		return CustomCostResourceTypeProp, nil
	case strings.TrimSpace(strings.ToLower(CustomCostProviderIdProp)):
		return CustomCostProviderIdProp, nil
	case strings.TrimSpace(strings.ToLower(CustomCostUsageUnitProp)):
		return CustomCostUsageUnitProp, nil
	case strings.TrimSpace(strings.ToLower(CustomCostDomainProp)):
		return CustomCostDomainProp, nil
	case strings.TrimSpace(strings.ToLower(CustomCostCostSourceProp)):
		return CustomCostCostSourceProp, nil
	}

	return "", fmt.Errorf("invalid custom cost property: %s", text)
}
