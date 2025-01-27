package thanos

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/modules/prometheus-source/pkg/env"
)

var (
	lock           = new(sync.Mutex)
	enabled        = env.IsThanosEnabled()
	queryUrl       = env.GetThanosQueryUrl()
	offset         = env.GetThanosOffset()
	maxSourceRes   = env.GetThanosMaxSourceResolution()
	offsetDuration *time.Duration
	queryOffset    = fmt.Sprintf(" offset %s", offset)
)

// IsEnabled returns true if Thanos is enabled.
func IsEnabled() bool {
	return enabled
}

// QueryURL returns true if Thanos is enabled.
func QueryURL() string {
	return queryUrl
}

// Offset returns the duration string for the query offset that should be applied to thanos
func Offset() string {
	return offset
}

// OffsetDuration returns the Offset as a parsed duration
func OffsetDuration() time.Duration {
	lock.Lock()
	defer lock.Unlock()

	if offsetDuration == nil {
		d, err := time.ParseDuration(offset)
		if err != nil {
			d = 0
		}

		offsetDuration = &d
	}

	return *offsetDuration
}

// QueryOffset returns a string in the format: " offset %s" substituting in the Offset() string.
func QueryOffset() string {
	return queryOffset
}
