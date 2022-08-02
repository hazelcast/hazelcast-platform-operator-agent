package bucket

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Blob storage types
const (
	AWS   = "s3"
	GCP   = "gs"
	AZURE = "azblob"
)

// AWS
const (
	BucketDataS3AccessKeyID        = "access-key-id"
	BucketDataS3SecretAccessKey    = "secret-access-key"
	BucketDataS3Region             = "region"
	BucketDataS3EnvAccessKeyID     = "AWS_ACCESS_KEY_ID"
	BucketDataS3EnvSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	BucketDataS3EnvRegion          = "AWS_REGION"
)

// GCP
const (
	BucketDataGCPCredentialFile    = "google-credentials-path"
	BucketDataGCPEnvCredentialFile = "GOOGLE_APPLICATION_CREDENTIALS"
)

// Azure
const (
	BucketDataAzureStorageAccount    = "storage-account"
	BucketDataAzureStorageKey        = "storage-key"
	BucketDataAzureEnvStorageAccount = "AZURE_STORAGE_ACCOUNT"
	BucketDataAzureEnvStorageKey     = "AZURE_STORAGE_KEY"
)

func OpenBucket(ctx context.Context, bucketURL string, secretData map[string][]byte) (*blob.Bucket, error) {
	switch {
	case strings.HasPrefix(bucketURL, AWS):
		return openAWS(ctx, bucketURL, secretData)

	case strings.HasPrefix(bucketURL, GCP):
		return openGCP(ctx, bucketURL, secretData)

	case strings.HasPrefix(bucketURL, AZURE):
		return openAZURE(ctx, bucketURL, secretData)

	default:
		return blob.OpenBucket(ctx, bucketURL)
	}
}

func GetSecretData(ctx context.Context, sn string) (map[string][]byte, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	namespace, err := getNamespace()
	if err != nil {
		return nil, err
	}

	secret, err := clientset.CoreV1().Secrets(namespace).Get(ctx, sn, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return secret.Data, nil
}

func getNamespace() (string, error) {
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

func openAWS(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	if err := setCredentialEnv(secret, BucketDataS3AccessKeyID, BucketDataS3EnvAccessKeyID); err != nil {
		return nil, err
	}
	if err := setCredentialEnv(secret, BucketDataS3Region, BucketDataS3EnvRegion); err != nil {
		return nil, err
	}
	if err := setCredentialEnv(secret, BucketDataS3SecretAccessKey, BucketDataS3EnvSecretAccessKey); err != nil {
		return nil, err
	}

	return blob.OpenBucket(ctx, bucketURL)
}

func openGCP(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	value, ok := secret[BucketDataGCPCredentialFile]
	if !ok {
		return nil, fmt.Errorf("invalid secret for GCP : missing credential: %v", BucketDataGCPCredentialFile)
	}

	creds, err := google.CredentialsFromJSON(ctx, value, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, err
	}

	client, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		gcp.CredentialsTokenSource(creds),
	)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(bucketURL)
	if err != nil {
		return nil, err
	}

	bucket, err := gcsblob.OpenBucket(ctx, client, u.Host, nil)
	if err != nil {
		return nil, err
	}

	return blob.PrefixedBucket(bucket, u.Query().Get("prefix")), nil
}

func openAZURE(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	if err := setCredentialEnv(secret, BucketDataAzureStorageAccount, BucketDataAzureEnvStorageAccount); err != nil {
		return nil, err
	}

	if err := setCredentialEnv(secret, BucketDataAzureStorageKey, BucketDataAzureEnvStorageKey); err != nil {
		return nil, err
	}

	return blob.OpenBucket(ctx, bucketURL)
}

func setCredentialEnv(secret map[string][]byte, key, name string) error {
	value, ok := secret[key]
	if !ok {
		return fmt.Errorf("invalid secret: missing key: %v", key)
	}
	return os.Setenv(name, string(value))
}
