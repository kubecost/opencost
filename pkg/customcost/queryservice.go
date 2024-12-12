package customcost

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"go.opentelemetry.io/otel"
)

const tracerName = "github.com/opencost/opencost/pkg/customcost"

type QueryService struct {
	Querier Querier
}

func NewQueryService(querier Querier) *QueryService {
	return &QueryService{
		Querier: querier,
	}
}

func (qs *QueryService) GetCustomCostTotalHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCustomCostTotalHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if qs == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if qs.Querier == nil {
			http.Error(w, "CustomCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCustomCostTotalRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := qs.Querier.QueryTotal(ctx, *request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		_, spanResp := tracer.Start(ctx, "write response")
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteData(w, resp)
		spanResp.End()
	}
}

func (qs *QueryService) GetCustomCostTimeseriesHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCustomCostTimeseriesHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if qs == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if qs.Querier == nil {
			http.Error(w, "CustomCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCustomCostTimeseriesRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := qs.Querier.QueryTimeseries(ctx, *request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		_, spanResp := tracer.Start(ctx, "write response")
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteData(w, resp)
		spanResp.End()
	}
}
