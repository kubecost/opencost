package customcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
)

// Repository is an interface for storing and retrieving CustomCost data
type Repository interface {
	Has(time.Time, string) (bool, error)
	Get(time.Time, string) (*pb.CustomCostResponse, error)
	Keys() ([]string, error)
	Put(*pb.CustomCostResponse) error
	Expire(time.Time) error
}
