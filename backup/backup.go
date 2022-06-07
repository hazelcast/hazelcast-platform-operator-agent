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
	"strings"

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

	"github.com/hazelcast/platform-operator-agent/util"
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

		seq := util.ConvertHumanReadableFormat(s.Name())

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
	provider, _, err := util.ValidateBucketURL(bucketURL)
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

	namespace, err := util.GetNamespace()
	if err != nil {
		return nil, err
	}

	secret, err := clientset.CoreV1().Secrets(namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	switch provider {
	case util.AWS:
		return openAWS(ctx, bucketURL, secret.Data)

	case util.GCP:
		return openGCP(ctx, bucketURL, secret.Data)

	case util.AZURE:
		return openAZURE(ctx, bucketURL, secret.Data)

	default:
		return nil, fmt.Errorf("invalid bucket path")
	}
}

func openAWS(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	if err := setCredentialEnv(secret, util.BucketDataS3AccessKeyID, util.BucketDataS3EnvAccessKeyID); err != nil {
		return nil, err
	}
	if err := setCredentialEnv(secret, util.BucketDataS3Region, util.BucketDataS3EnvRegion); err != nil {
		return nil, err
	}
	if err := setCredentialEnv(secret, util.BucketDataS3SecretAccessKey, util.BucketDataS3EnvSecretAccessKey); err != nil {
		return nil, err
	}

	return blob.OpenBucket(ctx, bucketURL)
}

func openGCP(ctx context.Context, bucketURL string, secret map[string][]byte) (*blob.Bucket, error) {
	_, bucketName, err := util.ValidateBucketURL(bucketURL)
	if err != nil {
		return nil, err
	}

	value, ok := secret[util.BucketDataGCPCredentialFile]
	if !ok {
		return nil, fmt.Errorf("invalid secret for GCP : missing credential: %v", util.BucketDataGCPCredentialFile)
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
	if err := setCredentialEnv(secret, util.BucketDataAzureStorageAccount, util.BucketDataAzureEnvStorageAccount); err != nil {
		return nil, err
	}

	if err := setCredentialEnv(secret, util.BucketDataAzureStorageKey, util.BucketDataAzureEnvStorageKey); err != nil {
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
