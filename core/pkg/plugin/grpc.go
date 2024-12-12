package plugin

import (
	"context"

	"github.com/opencost/opencost/core/pkg/model/pb"
)

// GRPCClient is an implementation of CustomCostsSource that talks over RPC.
type GRPCClient struct{ client pb.CustomCostsSourceClient }

func (m *GRPCClient) GetCustomCosts(req *pb.CustomCostRequest) []*pb.CustomCostResponse {
	resp, err := m.client.GetCustomCosts(context.Background(), req)
	if err != nil {
		return []*pb.CustomCostResponse{
			{
				Errors: []string{err.Error()},
			},
		}
	}
	derefs := []*pb.CustomCostResponse{}
	for _, resp := range resp.Resps {
		derefs = append(derefs, resp)
	}
	return derefs
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	pb.UnimplementedCustomCostsSourceServer
	// This is the real implementation
	Impl CustomCostSource
}

func (m *GRPCServer) GetCustomCosts(
	ctx context.Context,
	req *pb.CustomCostRequest) (*pb.CustomCostResponseSet, error) {
	ptrs := []*pb.CustomCostResponse{}
	costs := m.Impl.GetCustomCosts(req)
	for _, cost := range costs {
		ptrs = append(ptrs, cost)
	}
	return &pb.CustomCostResponseSet{
		Resps: ptrs,
	}, nil
}
