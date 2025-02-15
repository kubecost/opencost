package linode

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linode/linodego"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	"golang.org/x/oauth2"
)

const (
	InstanceAPIPricing = "Instance API Pricing"
	linodeTokenEnv     = "LINODE_TOKEN"
	linodeAPITimeout   = 30 * time.Second
)

var ErrTokenNotFound = errors.New("token for Linode API not found")

var initOnce = &sync.Once{}

type LinodePricing map[string]*linodego.LinodeType

type Linode struct {
	Clientset        clustercache.ClusterCache
	Config           models.ProviderConfig
	ClusterRegion    string
	ClusterAccountID string
	ClusterProjectID string

	linodeClient  atomic.Value
	isHaCluster   atomic.Bool
	regions       atomic.Value
	linodePricing sync.Map
}

var _ models.Provider = (*Linode)(nil)

func (l *Linode) init() error {
	clusterID, err := strconv.Atoi(strings.TrimLeft(l.ClusterProjectID, "lke"))
	if err != nil {
		log.Errorf("Could not parse LKE cluster ID (%s): %+v", l.ClusterProjectID, err)

		return err
	}

	linodeToken := os.Getenv(linodeTokenEnv)
	if linodeToken == "" {
		log.Errorf("Could not get Linode token from environment variables")

		return fmt.Errorf("%w: env:%s", err, linodeTokenEnv)
	}

	httpClient := http.Client{Timeout: linodeAPITimeout}
	if linodeToken != "" {
		httpClient.Transport = &oauth2.Transport{
			Source: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: linodeToken}),
		}
	}

	initOnce.Do(func() {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			first := true

			for {
				lc := linodego.NewClient(&httpClient)
				l.linodeClient.Store(lc)

				sleepTime := time.Hour

				if err := l.refreshPricing(clusterID); err != nil {
					sleepTime = time.Minute
				}

				if first {
					first = false
					wg.Done()
				}

				time.Sleep(sleepTime)
			}
		}()
		wg.Wait()
	})

	return nil
}

func (l *Linode) ClusterInfo() (map[string]string, error) {
	c, err := l.GetConfig()
	if err != nil {
		log.Errorf("Config not found for %s", env.GetClusterID())

		return nil, err
	}

	meta := map[string]string{
		"name":              "Linode Cluster #1",
		"provider":          opencost.LinodeProvider,
		"provisioner":       "LKE",
		"region":            l.ClusterRegion,
		"account":           l.ClusterAccountID,
		"project":           l.ClusterProjectID,
		"remoteReadEnabled": strconv.FormatBool(env.IsRemoteEnabled()),
		"id":                env.GetClusterID(),
	}

	if c.ClusterName != "" {
		meta["name"] = c.ClusterName
	}

	return meta, nil
}

func (l *Linode) Regions() []string {
	regionOverrides := env.GetRegionOverrideList()
	if len(regionOverrides) > 0 {
		log.Debugf("Overriding Linode regions with configured region list: %+v", regionOverrides)

		return regionOverrides
	}

	return l.regions.Load().([]string)
}

func (l *Linode) GetManagementPlatform() (string, error) {
	nodes := l.Clientset.GetAllNodes()

	if len(nodes) > 0 {
		n := nodes[0]
		if _, ok := n.Labels["topology.linode.com/region"]; ok {
			return "Linode", nil
		}
		if _, ok := n.Labels["lke.linode.com/pool-id"]; ok {
			return "Linode", nil
		}
	}

	return "", nil
}

func (l *Linode) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{
		Checks: []*models.ServiceAccountCheck{},
	}
}

func (*Linode) ApplyReservedInstancePricing(map[string]*models.Node) {}

func (*Linode) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

func (*Linode) GetLocalStorageQuery(window, offset time.Duration, rate bool, used bool) string {
	return ""
}

func (*Linode) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (*Linode) GetDisks() ([]byte, error) {
	return nil, nil
}
