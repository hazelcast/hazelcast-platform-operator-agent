package bucket

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
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

func OpenBucket(ctx context.Context, bucketURL, secretName string) (*blob.Bucket, error) {
	provider, _, err := validateBucketURL(bucketURL)
	if err != nil {
		return nil, err
	}

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

	secret, err := clientset.CoreV1().Secrets(namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	switch provider {
	case AWS:
		return openAWS(ctx, bucketURL, secret.Data)

	case GCP:
		return openGCP(ctx, bucketURL, secret.Data)

	case AZURE:
		return openAZURE(ctx, bucketURL, secret.Data)

	default:
		return nil, fmt.Errorf("invalid bucket path")
	}
}

func validateBucketURL(bucketURL string) (string, string, error) {
	r, _ := regexp.Compile(fmt.Sprintf("^(%s|%s|%s)://(.+)$", AWS, GCP, AZURE))
	if !r.MatchString(bucketURL) {
		return "", "", fmt.Errorf("invalid BucketURL: %v", bucketURL)
	}
	subMatch := r.FindStringSubmatch(bucketURL)

	provider := subMatch[1]
	bucketName := strings.Split(subMatch[2], "?")[0]

	return provider, bucketName, nil
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
	_, bucketName, err := validateBucketURL(bucketURL)
	if err != nil {
		return nil, err
	}

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

	return gcsblob.OpenBucket(ctx, client, bucketName, nil)
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
