package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/patrickmn/go-cache"
)

// InitializeSettingsPubSub sets up the pub/sub mechanisms and kicks of
// routines to detect and publish changes, as well as some routines that
// subscribe and take actions.
func (a *Accesses) InitializeSettingsPubSub() {
	a.settingsSubscribers = map[string][]chan string{}

	// Publish settings changes
	go func(a *Accesses) {
		for {
			// Publish changes to custom pricing
			if a.customPricingHasChanged() {
				for _, ch := range a.settingsSubscribers[CustomPricingSetting] {
					if data, ok := a.SettingsCache.Get(CustomPricingSetting); ok {
						if cpStr, ok := data.(string); ok {
							ch <- cpStr
						}
					}
				}
			}

			// Publish changes to discount
			if a.discountHasChanged() {
				for _, ch := range a.settingsSubscribers[DiscountSetting] {
					if data, ok := a.SettingsCache.Get(DiscountSetting); ok {
						if discStr, ok := data.(string); ok {
							ch <- discStr
						}
					}
				}
			}

			time.Sleep(500 * time.Millisecond)
		}
	}(a)

	// Clear caches when custom pricing or discount changes
	go func(a *Accesses) {
		costDataCacheCh := make(chan string)
		a.SubscribeToCustomPricingChanges(costDataCacheCh)
		a.SubscribeToDiscountChanges(costDataCacheCh)
		for {
			msg := <-costDataCacheCh
			log.Infof("Flushing cost data caches: %s", msg)
			a.AggregateCache.Flush()
			a.CostDataCache.Flush()
		}
	}(a)
}

// SubscribeToCustomPricingChanges subscribes the given channel to receive
// custom pricing changes.
func (a *Accesses) SubscribeToCustomPricingChanges(ch chan string) {
	a.settingsMutex.Lock()
	defer a.settingsMutex.Unlock()

	a.settingsSubscribers[CustomPricingSetting] = append(a.settingsSubscribers[CustomPricingSetting], ch)
}

// SubscribeToDiscountChanges subscribes the given channel to receive discount
// changes.
func (a *Accesses) SubscribeToDiscountChanges(ch chan string) {
	a.settingsMutex.Lock()
	defer a.settingsMutex.Unlock()

	a.settingsSubscribers[DiscountSetting] = append(a.settingsSubscribers[DiscountSetting], ch)
}

// customPricingHasChanged returns true if custom pricing settings have changed
// since the last time this function was called.
func (a *Accesses) customPricingHasChanged() bool {
	customPricing, err := a.CloudProvider.GetConfig()
	if err != nil || customPricing == nil {
		log.Errorf("error accessing cloud provider configuration: %s", err)
		return false
	}

	// describe parameters by which we determine whether or not custom
	// pricing settings have changed
	encodeCustomPricing := func(cp *models.CustomPricing) string {
		return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%s:%s", cp.CustomPricesEnabled, cp.CPU, cp.SpotCPU,
			cp.RAM, cp.SpotRAM, cp.GPU, cp.Storage, cp.CurrencyCode, cp.SharedOverhead)
	}

	// compare cached custom pricing parameters with current values
	cpStr := encodeCustomPricing(customPricing)
	cpStrCached := ""
	val, found := a.SettingsCache.Get(CustomPricingSetting)
	if !found {
		// if no settings are found (e.g. upon first call) cache custom pricing settings but
		// return false, as nothing has "changed" per se
		a.SettingsCache.Set(CustomPricingSetting, cpStr, cache.NoExpiration)
		return false
	}
	cpStrCached, ok := val.(string)
	if !ok {
		log.Errorf("caching error: failed to cast custom pricing to string")
	}
	if cpStr == cpStrCached {
		return false
	}

	// cache new custom pricing settings
	a.SettingsCache.Set(CustomPricingSetting, cpStr, cache.DefaultExpiration)

	return true
}

// discountHasChanged returns true if discount settings have changed
// since the last time this function was called.
func (a *Accesses) discountHasChanged() bool {
	customPricing, err := a.CloudProvider.GetConfig()
	if err != nil || customPricing == nil {
		log.Errorf("error accessing cloud provider configuration: %s", err)
		return false
	}

	// describe parameters by which we determine whether or not custom
	// pricing settings have changed
	encodeDiscount := func(cp *models.CustomPricing) string {
		return fmt.Sprintf("%s:%s", cp.Discount, cp.NegotiatedDiscount)
	}

	// compare cached custom pricing parameters with current values
	discStr := encodeDiscount(customPricing)
	discStrCached := ""
	val, found := a.SettingsCache.Get(DiscountSetting)
	if !found {
		// if no settings are found (e.g. upon first call) cache custom pricing settings but
		// return false, as nothing has "changed" per se
		a.SettingsCache.Set(DiscountSetting, discStr, cache.NoExpiration)
		return false
	}
	discStrCached, ok := val.(string)
	if !ok {
		log.Errorf("caching error: failed to cast discount to string")
	}
	if discStr == discStrCached {
		return false
	}

	// cache new custom pricing settings
	a.SettingsCache.Set(DiscountSetting, discStr, cache.DefaultExpiration)

	return true
}
