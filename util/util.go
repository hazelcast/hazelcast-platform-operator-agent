package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

func ValidateBucketURL(bucketURL string) (string, string, error) {

	r, _ := regexp.Compile(fmt.Sprintf("^(%s|%s|%s)://(.+)$", AWS, GCP, AZURE))
	if !r.MatchString(bucketURL) {
		return "", "", fmt.Errorf("invalid BucketURL: %v", bucketURL)
	}
	subMatch := r.FindStringSubmatch(bucketURL)

	provider := subMatch[1]
	bucketName := subMatch[2]
	return provider, bucketName, nil
}

func GetNamespace() (string, error) {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns, nil
	}
	if data, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
			return ns, nil
		}
		return "", err
	}
	return "", nil
}
