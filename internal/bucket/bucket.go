package bucket

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
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
	S3AccessKeyID        = "access-key-id"
	S3SecretAccessKey    = "secret-access-key"
	S3Region             = "region"
	S3EnvAccessKeyID     = "AWS_ACCESS_KEY_ID"
	S3EnvSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	S3EnvRegion          = "AWS_REGION"
)

// GCP
const (
	GCPCredentialFile = "google-credentials-path"
)

// Azure
const (
	AzureStorageAccount    = "storage-account"
	AzureStorageKey        = "storage-key"
	AzureEnvStorageAccount = "AZURE_STORAGE_ACCOUNT"
	AzureEnvStorageKey     = "AZURE_STORAGE_KEY"
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

func SecretData(ctx context.Context, sn string) (map[string][]byte, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	namespace, err := namespace()
	if err != nil {
		return nil, err
	}

	secret, err := clientset.CoreV1().Secrets(namespace).Get(ctx, sn, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return secret.Data, nil
}

func namespace() (string, error) {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns, nil
	}
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
			return ns, nil
		}
		return "", err
	}
	return "", nil
}

func openAWS(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	if err := setCredentialEnv(secret, S3AccessKeyID, S3EnvAccessKeyID); err != nil {
		return nil, err
	}
	if err := setCredentialEnv(secret, S3Region, S3EnvRegion); err != nil {
		return nil, err
	}
	if err := setCredentialEnv(secret, S3SecretAccessKey, S3EnvSecretAccessKey); err != nil {
		return nil, err
	}

	return blob.OpenBucket(ctx, bucketURL)
}

func openGCP(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	value, ok := secret[GCPCredentialFile]
	if !ok {
		return nil, fmt.Errorf("invalid secret for GCP : missing credential: %v", GCPCredentialFile)
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
	if err := setCredentialEnv(secret, AzureStorageAccount, AzureEnvStorageAccount); err != nil {
		return nil, err
	}

	if err := setCredentialEnv(secret, AzureStorageKey, AzureEnvStorageKey); err != nil {
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

func SaveFileFromBucket(ctx context.Context, bucket *blob.Bucket, key, path string) error {
	s, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return err
	}
	defer s.Close()

	destPath := filepath.Join(path, key)

	d, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer d.Close()

	if _, err = io.Copy(d, s); err != nil {
		return err
	}

	// flush file
	if err = d.Sync(); err != nil {
		return err
	}

	return nil
}
