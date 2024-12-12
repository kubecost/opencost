package clusters

import (
	"errors"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
)

// DataEnvelope is a generic wrapper struct for http response data
type DataEnvelope struct {
	Code   int         `json:"code"`
	Status string      `json:"status"`
	Data   interface{} `json:"data"`
}

// ClusterManagerHTTPService is an implementation of HTTPService which provides
// the frontend with the ability to manage stored cluster definitions.
type ClusterManagerHTTPService struct {
	manager *ClusterManager
}

// NewClusterManagerHTTPService creates a new cluster management http service
func NewClusterManagerHTTPService(manager *ClusterManager) *ClusterManagerHTTPService {
	return &ClusterManagerHTTPService{
		manager: manager,
	}
}

// Register assigns the endpoints and returns an error on failure.
func (cme *ClusterManagerHTTPService) Register(router *httprouter.Router) error {
	router.GET("/clusters", cme.GetAllClusters)
	router.PUT("/clusters", cme.PutCluster)
	router.DELETE("/clusters/:id", cme.DeleteCluster)

	return nil
}

func (cme *ClusterManagerHTTPService) GetAllClusters(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	clusters := cme.manager.GetAll()
	w.Write(wrapData(clusters, nil))
}

func (cme *ClusterManagerHTTPService) PutCluster(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	data, err := io.ReadAll(r.Body)
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}

	var clusterDef ClusterDefinition
	err = json.Unmarshal(data, &clusterDef)
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}

	cd, err := cme.manager.AddOrUpdate(clusterDef)
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}

	w.Write(wrapData(cd, nil))
}

func (cme *ClusterManagerHTTPService) DeleteCluster(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	clusterID := ps.ByName("id")
	if clusterID == "" {
		w.Write(wrapData(nil, errors.New("Failed to locate cluster with empty id.")))
		return
	}

	err := cme.manager.Remove(clusterID)
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}

	w.Write(wrapData("success", nil))
}

func wrapData(data interface{}, err error) []byte {
	var resp []byte

	if err != nil {
		log.Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&DataEnvelope{
			Code:   http.StatusInternalServerError,
			Status: "error",
			Data:   err.Error(),
		})
	} else {
		resp, _ = json.Marshal(&DataEnvelope{
			Code:   http.StatusOK,
			Status: "success",
			Data:   data,
		})

	}

	return resp
}
