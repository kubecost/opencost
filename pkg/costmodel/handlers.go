package costmodel

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	assetfilter "github.com/opencost/opencost/core/pkg/filter/asset"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/pkg/carbon"
	"github.com/opencost/opencost/pkg/env"
)

// ComputeAllocationHandler returns the assets from the CostModel.
func (a *Accesses) ComputeAssetsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	qp := httputil.NewQueryParams(r.URL.Query())

	// Window is a required field describing the window of time over which to
	// compute allocation data.
	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
		return
	}

	filterString := qp.Get("filter", "")

	assetSet, err := a.computeAssetsFromCostmodel(window, filterString)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting assets: %s", err), http.StatusInternalServerError)
		return
	}

	w.Write(WrapData(assetSet, nil))
}

// ComputeAllocationHandler returns the assets from the CostModel.
func (a *Accesses) ComputeAssetsCarbonHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	qp := httputil.NewQueryParams(r.URL.Query())

	// Window is a required field describing the window of time over which to
	// compute allocation data.
	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
		return
	}

	filterString := qp.Get("filter", "")

	assetSet, err := a.computeAssetsFromCostmodel(window, filterString)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting assets: %s", err), http.StatusInternalServerError)
		return
	}

	carbonEstimates, err := carbon.RelateCarbonAssets(assetSet)

	w.Write(WrapData(carbonEstimates, nil))
}

func (a *Accesses) computeAssetsFromCostmodel(window opencost.Window, filterString string) (*opencost.AssetSet, error) {

	assetSet, err := a.Model.ComputeAssets(*window.Start(), *window.End())
	if err != nil {
		return nil, fmt.Errorf("error computing asset set: %s", err)
	}

	var filter opencost.AssetMatcher
	if filterString == "" {
		filter = &matcher.AllPass[opencost.Asset]{}
	} else {
		parser := assetfilter.NewAssetFilterParser()
		tree, errParse := parser.Parse(filterString)
		if errParse != nil {
			return nil, fmt.Errorf("err parsing filter '%s': %v", ast.ToPreOrderShortString(tree), errParse)
		}
		compiler := opencost.NewAssetMatchCompiler()
		var err error
		filter, err = compiler.Compile(tree)
		if err != nil {
			return nil, fmt.Errorf("err compiling filter '%s': %v", ast.ToPreOrderShortString(tree), err)
		}
	}
	if filter == nil {
		return nil, fmt.Errorf("unexpected nil filter")
	}

	for key, asset := range assetSet.Assets {
		if !filter.Matches(asset) {
			delete(assetSet.Assets, key)
		}
	}

	return assetSet, nil
}
