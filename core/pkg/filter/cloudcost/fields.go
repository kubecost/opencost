package cloudcost

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

// CloudCostField is an enum that represents CloudCost specific fields that can be filtered
type CloudCostField string

const (
	FieldInvoiceEntityID   CloudCostField = CloudCostField(fieldstrings.FieldInvoiceEntityID)
	FieldInvoiceEntityName CloudCostField = CloudCostField(fieldstrings.FieldInvoiceEntityName)
	FieldAccountID         CloudCostField = CloudCostField(fieldstrings.FieldAccountID)
	FieldAccountName       CloudCostField = CloudCostField(fieldstrings.FieldAccountName)
	FieldRegionID          CloudCostField = CloudCostField(fieldstrings.FieldRegionID)
	FieldAvailabilityZone  CloudCostField = CloudCostField(fieldstrings.FieldAvailabilityZone)
	FieldProvider          CloudCostField = CloudCostField(fieldstrings.FieldProvider)
	FieldProviderID        CloudCostField = CloudCostField(fieldstrings.FieldProviderID)
	FieldCategory          CloudCostField = CloudCostField(fieldstrings.FieldCategory)
	FieldService           CloudCostField = CloudCostField(fieldstrings.FieldService)
	FieldLabel             CloudCostField = CloudCostField(fieldstrings.FieldLabel)
)
