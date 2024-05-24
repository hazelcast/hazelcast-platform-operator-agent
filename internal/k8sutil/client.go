package k8sutil

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func Client() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
