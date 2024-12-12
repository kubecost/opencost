package opencost

import (
	"time"

	filter "github.com/opencost/opencost/core/pkg/filter/legacy"
	"github.com/opencost/opencost/core/pkg/log"
)

// Coverage This is a placeholder struct which can be replaced by a more specific implementation later
type Coverage struct {
	Window   Window    `json:"window"`
	Type     string    `json:"type"`
	Count    int       `json:"count"`
	Updated  time.Time `json:"updated"`
	Errors   []string  `json:"errors"`
	Warnings []string  `json:"warnings"`
}

func (c *Coverage) GetWindow() Window {
	return c.Window
}

func (c *Coverage) Key() string {
	return c.Type
}

func (c *Coverage) IsEmpty() bool {
	if c == nil {
		log.Warnf("calling IsEmpty() on a nil Coverage")
		return true
	}
	return c.Type == "" && c.Count == 0 && len(c.Errors) == 0 && len(c.Warnings) == 0 && c.Updated == time.Time{}
}

func (c *Coverage) Clone() *Coverage {
	if c == nil {
		log.Warnf("calling Clone() on a nil Coverage")
		return nil
	}
	var errors []string
	if len(c.Errors) > 0 {
		errors = make([]string, len(c.Errors))
		copy(errors, c.Errors)
	}
	var warnings []string
	if len(c.Warnings) > 0 {
		warnings = make([]string, len(c.Warnings))
		copy(warnings, c.Warnings)
	}
	return &Coverage{
		Window:   c.Window.Clone(),
		Type:     c.Type,
		Count:    c.Count,
		Updated:  c.Updated,
		Errors:   errors,
		Warnings: warnings,
	}
}

// Coverage This is a placeholder struct which can be replaced by a more specific implementation later
type CoverageSet struct {
	Window Window               `json:"window"`
	Items  map[string]*Coverage `json:"items"`
}

func NewCoverageSet(start, end time.Time) *CoverageSet {
	return &CoverageSet{
		Window: NewWindow(&start, &end),
		Items:  map[string]*Coverage{},
	}
}

func (cs *CoverageSet) GetWindow() Window {
	return cs.Window
}

func (cs *CoverageSet) IsEmpty() bool {
	if cs == nil {
		log.Warnf("calling IsEmpty() on a nil CoverageSet")
		return true
	}
	for _, item := range cs.Items {
		if !item.IsEmpty() {
			return false
		}
	}
	return true
}

func (cs *CoverageSet) Clone() *CoverageSet {
	var items map[string]*Coverage
	if cs.Items != nil {
		items = make(map[string]*Coverage, len(cs.Items))
		for k, item := range cs.Items {
			items[k] = item.Clone()
		}

	}
	return &CoverageSet{
		Window: cs.Window.Clone(),
		Items:  items,
	}
}

func (cs *CoverageSet) Insert(coverage *Coverage) {
	if cs.Items == nil {
		cs.Items = map[string]*Coverage{}
	}
	cs.Items[coverage.Key()] = coverage
}

func (cs *CoverageSet) Filter(filters filter.Filter[*Coverage]) *CoverageSet {
	if cs == nil {
		return nil
	}

	if filters == nil {
		return cs.Clone()
	}

	result := NewCoverageSet(*cs.Window.start, *cs.Window.end)

	for _, c := range cs.Items {
		if filters.Matches(c) {
			result.Insert(c.Clone())
		}
	}

	return result
}
