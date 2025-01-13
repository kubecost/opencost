package linode

import (
	"context"
	"errors"
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
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	InstanceAPIPricing = "Instance API Pricing"

	kubeAPITimeout   = 30 * time.Second
	linodeAPITimeout = 30 * time.Second
)

var initOnce = &sync.Once{}

type LinodePricing map[string]*linodego.LinodeType

type Linode struct {
	KubeClient       kubernetes.Interface
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

func (l *Linode) init() error {
	cpricing, err := l.Config.GetCustomPricingData()
	if err != nil {
		log.Errorf("Could not get Linode custom pricing: %+v", err)

		return err
	}

	clusterID, err := strconv.Atoi(strings.TrimLeft(l.ClusterProjectID, "lke"))
	if err != nil {
		log.Errorf("Could not parse LKE cluster ID (%s): %+v", l.ClusterProjectID, err)

		return err
	}

	initOnce.Do(func() {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			first := true

			for {
				linodeToken := os.Getenv("LINODE_TOKEN")
				if cpricing.LinodeTokenSecret != "" && linodeToken == "" {
					namespace := "default"
					name := cpricing.LinodeTokenSecret
					if secretName := strings.Split(cpricing.LinodeTokenSecret, "/"); len(secretName) > 1 {
						namespace = secretName[0]
						name = secretName[1]
					}

					ctx, cancel := context.WithTimeout(context.Background(), kubeAPITimeout)
					if apiToken, err := l.KubeClient.CoreV1().Secrets(namespace).Get(ctx, name, v1.GetOptions{}); err != nil {
						log.Errorf("Could not fetch token via secret (%s): %+v", cpricing.LinodeTokenSecret, err)
					} else {
						linodeToken = string(apiToken.Data["token"])
					}
					cancel()
				}

				httpClient := http.Client{Timeout: linodeAPITimeout}
				if linodeToken != "" {
					httpClient.Transport = &oauth2.Transport{
						Source: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: linodeToken}),
					}
				}

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
			return "linode", nil
		}
		if _, ok := n.Labels["lke.linode.com/pool-id"]; ok {
			return "linode", nil
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
