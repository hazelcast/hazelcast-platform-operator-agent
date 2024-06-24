package bucket

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/hazelcast/platform-operator-agent/internal/k8s"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
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

func OpenBucket(ctx context.Context, bucketURL string, secretName string) (*blob.Bucket, error) {
	var secretData map[string][]byte

	// if secretName is not empty, then read the bucket authentication secret from the given secret.
	if secretName != "" {
		sr, err := newSecretReader()
		if err != nil {
			return nil, err
		}
		secretData, err = sr.secretData(ctx, secretName)
		if err != nil {
			return nil, err
		}
	}

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

type secretReader struct {
	clientcorev1.SecretInterface
}

func newSecretReader() (*secretReader, error) {
	c, err := k8s.Client()
	if err != nil {
		return nil, err
	}

	ns, err := k8s.Namespace()
	if err != nil {
		return nil, err
	}

	return &secretReader{SecretInterface: c.CoreV1().Secrets(ns)}, nil
}

func (sr secretReader) secretData(ctx context.Context, sn string) (map[string][]byte, error) {
	secret, err := sr.Get(ctx, sn, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if len(secret.Data) == 0 {
		return nil, fmt.Errorf("the data in the bucket authentication secret is empty: %s", secret.Name)
	}

	return secret.Data, nil
}

func openAWS(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	if secret == nil {
		return openAWSWithSession(ctx, bucketURL)
	}

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

func openAWSWithSession(ctx context.Context, bucketURL string) (*blob.Bucket, error) {
	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(bucketURL)
	if err != nil {
		return nil, err
	}

	bucket, err := s3blob.OpenBucket(ctx, s, u.Host, nil)
	if err != nil {
		return nil, err
	}

	return blob.PrefixedBucket(bucket, u.Query().Get("prefix")), nil
}

func openGCP(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	creds, err := credentials(ctx, secret)
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

const scope = "https://www.googleapis.com/auth/cloud-platform"

func credentials(ctx context.Context, secret map[string][]byte) (*google.Credentials, error) {
	if secret == nil {
		return google.FindDefaultCredentials(ctx, scope)
	}

	value, ok := secret[GCPCredentialFile]
	if !ok {
		return nil, fmt.Errorf("invalid secret for GCP : missing credential: %v", GCPCredentialFile)
	}

	return google.CredentialsFromJSON(ctx, value, scope)
}

func openAZURE(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	if secret != nil {
		if err := setCredentialEnv(secret, AzureStorageAccount, AzureEnvStorageAccount); err != nil {
			return nil, err
		}

		if err := setCredentialEnv(secret, AzureStorageKey, AzureEnvStorageKey); err != nil {
			return nil, err
		}
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

func DownloadFile(ctx context.Context, src, dst, filename string, secretName string) error {
	b, err := OpenBucket(ctx, src, secretName)
	if err != nil {
		return err
	}
	defer b.Close()

	exists, err := b.Exists(ctx, filename)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("not found: jar with the name not found: %v", filename)
	}

	if err = saveFile(ctx, b, filename, dst); err != nil {
		return err
	}

	return b.Close()
}

func DownloadFiles(ctx context.Context, src, dst string, secretName string) error {
	b, err := OpenBucket(ctx, src, secretName)
	if err != nil {
		return err
	}
	defer b.Close()

	iter := b.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// we only want top level files and no files under sub-folders
		if path.Base(obj.Key) != obj.Key {
			continue
		}

		if err = saveFile(ctx, b, obj.Key, dst); err != nil {
			return err
		}
	}

	return b.Close()
}

type BundleReq struct {
	URL        string `json:"url"`
	SecretName string `json:"secret_name"`
	DestDir    string `json:"dest_dir"`
}

func DownloadBundle(ctx context.Context, req BundleReq) error {
	bucketURI, err := uri.NormalizeURI(req.URL)
	if err != nil {
		return err
	}

	f, err := os.Create(filepath.Clean(req.DestDir))
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := OpenBucket(ctx, bucketURI, req.SecretName)
	if err != nil {
		return err
	}
	defer b.Close()

	w := zip.NewWriter(f)
	iter := b.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// we only want top level files and no files under sub-folders
		if path.Base(obj.Key) != obj.Key {
			continue
		}
		if err = addToZip(ctx, b, obj, w); err != nil {
			return err
		}
	}
	return w.Close()
}

func addToZip(ctx context.Context, b *blob.Bucket, obj *blob.ListObject, w *zip.Writer) error {
	r, err := b.NewReader(ctx, obj.Key, nil)
	if err != nil {
		return err
	}
	defer r.Close()
	f, err := w.Create(obj.Key)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	if err != nil {
		return err
	}
	return nil
}

func saveFile(ctx context.Context, bucket *blob.Bucket, key, path string) error {
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

	return s.Close()
}

func RemoveFile(ctx context.Context, bucket, key string, secretName string) error {
	b, err := OpenBucket(ctx, bucket, secretName)
	if err != nil {
		return err
	}
	defer b.Close()

	return b.Delete(ctx, key)
}
