package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/s3blob"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
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

var ErrEmptyBackupDir = errors.New("empty backup directory")

func UploadBackup(ctx context.Context, bucket *blob.Bucket, bucketURL, backupsDir, prefix string) error {
	backupSeqs, err := ioutil.ReadDir(backupsDir)
	if err != nil {
		return err
	}

	if len(backupSeqs) == 0 {
		return ErrEmptyBackupDir
	}

	// iterate over <backup-dir>/backup-<backupSeq>/ dirs
	for _, s := range backupSeqs {
		seqDir := filepath.Join(backupsDir, s.Name())
		backupUUIDs, err := ioutil.ReadDir(seqDir)
		if err != nil {
			return err
		}

		seq := convertHumanReadableFormat(s.Name())

		// iterate over <backup-dir>/backup-<backupSeq>/<UUID> dirs
		for _, u := range backupUUIDs {
			uuidDir := filepath.Join(seqDir, u.Name())

			key := filepath.Join(prefix, seq, u.Name()+".tar.gz")
			if err := uploadBackup(ctx, bucket, key, uuidDir, u.Name()); err != nil {
				return err
			}

			// after uploading backup we can remove local copy
			if err := os.RemoveAll(uuidDir); err != nil {
				return err
			}
		}

		// we finished uploading backups
		if err := os.RemoveAll(seqDir); err != nil {
			return err
		}
	}

	return nil
}

func uploadBackup(ctx context.Context, bucket *blob.Bucket, name, backupDir, baseDir string) error {
	w, err := bucket.NewWriter(ctx, name, nil)
	if err != nil {
		return err
	}
	defer w.Close()

	g := gzip.NewWriter(w)
	defer g.Close()

	t := tar.NewWriter(g)
	defer t.Close()

	return filepath.Walk(backupDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}

		// make sure files are relative to baseDir
		header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, backupDir))

		if err := t.WriteHeader(header); err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(t, f)
		return err
	})
}

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

// convertHumanReadableFormat converts backup-sequenceID into human readable format.
// backup-1643801670242 --> 2022-02-18-14-57-44
func convertHumanReadableFormat(backupFolderName string) string {
	epochString := strings.ReplaceAll(backupFolderName, "backup-", "")
	timestamp, _ := strconv.ParseInt(epochString, 10, 64)
	t := time.UnixMilli(timestamp)
	return t.Format("2006-01-02-15-04-05")
}

func validateBucketURL(bucketURL string) (string, string, error) {
	r, _ := regexp.Compile(fmt.Sprintf("^(%s|%s|%s)://(.+)$", AWS, GCP, AZURE))
	if !r.MatchString(bucketURL) {
		return "", "", fmt.Errorf("invalid BucketURL: %v", bucketURL)
	}
	subMatch := r.FindStringSubmatch(bucketURL)

	provider := subMatch[1]
	bucketName := subMatch[2]
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
