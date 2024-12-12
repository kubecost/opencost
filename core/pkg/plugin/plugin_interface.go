package plugin

import (
	"context"

	"github.com/hashicorp/go-plugin"
	"github.com/opencost/opencost/core/pkg/model/pb"
	grpc "google.golang.org/grpc"
)

// plugin interface
type CustomCostSource interface {
	GetCustomCosts(req *pb.CustomCostRequest) []*pb.CustomCostResponse
}

type CustomCostPlugin struct {
	plugin.Plugin
	// Impl Injection
	Impl CustomCostSource
}

// this method is called for as part of the reference plugin implementation
// see https://github.com/hashicorp/go-plugin/blob/main/examples/basic/shared/greeter_interface.go#L59
// for context and details
func (p *CustomCostPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	pb.RegisterCustomCostsSourceServer(s, &GRPCServer{Impl: p.Impl})
	return nil
}

// this method is called for as part of the reference plugin implementation
// see https://github.com/hashicorp/go-plugin/blob/main/examples/basic/shared/greeter_interface.go#L63
// for context and details
func (CustomCostPlugin) GRPCClient(context context.Context, b *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{client: pb.NewCustomCostsSourceClient(c)}, nil
}
