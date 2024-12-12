package kubeconfig

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// LoadKubeconfig attempts to load a kubeconfig based on default locations.
// If a path is passed in then only that path is checked and will error
// if not found
func LoadKubeconfig(path string) (*rest.Config, error) {
	// Use the default load order: KUBECONFIG env > $HOME/.kube/config > In cluster config
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if path != "" {
		loadingRules.ExplicitPath = path
	}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
	config, err := loader.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	config.UserAgent = "opencost"
	// use protobuf for faster serialization instead of default json
	// https://kubernetes.io/docs/reference/using-api/api-concepts/#alternate-representations-of-resources
	config.AcceptContentTypes = "application/vnd.kubernetes.protobuf,application/json"
	config.ContentType = "application/vnd.kubernetes.protobuf"
	return config, nil
}

// LoadKubeClient accepts a path to a kubeconfig to load and returns the clientset
func LoadKubeClient(path string) (*kubernetes.Clientset, error) {
	config, err := LoadKubeconfig(path)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}
