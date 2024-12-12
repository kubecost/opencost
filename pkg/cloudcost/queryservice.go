package cloudcost

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"go.opentelemetry.io/otel"
)

const tracerName = "github.com/opencost/ooencost/pkg/cloudcost"

const (
	csvFormat = "csv"
)

// QueryService surfaces endpoints for accessing CloudCost data in raw form or for display in views
type QueryService struct {
	Querier     Querier
	ViewQuerier ViewQuerier
}

func NewQueryService(querier Querier, viewQuerier ViewQuerier) *QueryService {
	return &QueryService{
		Querier:     querier,
		ViewQuerier: viewQuerier,
	}
}

func (s *QueryService) GetCloudCostHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCloudCostHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if s == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if s.Querier == nil {
			http.Error(w, "CloudCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCloudCostRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := s.Querier.Query(ctx, *request)
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

func (s *QueryService) GetCloudCostViewGraphHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCloudCostViewGraphHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if s == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if s.ViewQuerier == nil {
			http.Error(w, "CloudCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCloudCostViewRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := s.ViewQuerier.QueryViewGraph(ctx, *request)
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

func (s *QueryService) GetCloudCostViewTotalsHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCloudCostViewTotalsHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if s == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if s.ViewQuerier == nil {
			http.Error(w, "CloudCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCloudCostViewRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := s.ViewQuerier.QueryViewTotals(ctx, *request)
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

func (s *QueryService) GetCloudCostViewTableHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCloudCostViewTableHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if s == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if s.ViewQuerier == nil {
			http.Error(w, "CloudCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCloudCostViewRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		format := qp.Get("format", "json")
		if strings.HasPrefix(format, csvFormat) {
			w.Header().Set("Content-Type", "text/csv")
			w.Header().Set("Transfer-Encoding", "chunked")
		} else {
			// By default, send JSON
			w.Header().Set("Content-Type", "application/json")
		}

		resp, err := s.ViewQuerier.QueryViewTable(ctx, *request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		_, spanResp := tracer.Start(ctx, "write response")
		defer spanResp.End()
		if format == csvFormat {
			window := opencost.NewClosedWindow(request.Start, request.End)
			writeCloudCostViewTableRowsAsCSV(w, resp, window.String())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteData(w, resp)
	}
}
