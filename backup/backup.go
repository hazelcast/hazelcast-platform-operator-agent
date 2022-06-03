package backup

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

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

func UploadBackup(ctx context.Context, bucket *blob.Bucket, bucketURL, backupFolderPath, hazelcastCRName string) error {
	logger := util.GetLogger(ctx)

	backupFolderFileList, err := ioutil.ReadDir(backupFolderPath)
	if err != nil {
		logger.Errorf("Error occurred while read backup folder: %v", err)
		return nil
	}

	if len(backupFolderFileList) == 0 {
		logger.Printf("Backup folder doesn't have files/folders. Backup folder: %s", backupFolderPath)
		return nil
	}

	logger.Info("Uploading backup folders to the bucket...")

	for _, bf := range backupFolderFileList {
		backupItem := fmt.Sprintf("%s/%s", backupFolderPath, bf.Name())
		UUIDFolderList, err := ioutil.ReadDir(backupItem)
		if err != nil {
			logger.Errorf("Error occurred while read backup folder for timestamp: %v", err)
			return nil
		}
		for _, uf := range UUIDFolderList {
			zipFilePath := fmt.Sprintf("/%s/%s.zip", backupFolderPath, uf.Name())
			UUIDBackupFolderPath := path.Join(backupFolderPath, bf.Name(), uf.Name())

			if err := util.ZipFolder(UUIDBackupFolderPath, zipFilePath); err != nil {
				logger.Errorf("Couldn't zip hot backup folder: %s. Err: %v", UUIDBackupFolderPath, err)
				return nil
			}

			humanReadableBackupFolder := util.ConvertHumanReadableFormat(bf.Name())
			err = uploadBackupToBucket(ctx, bucket, fmt.Sprintf("%s/%s/%s.zip", hazelcastCRName, humanReadableBackupFolder, uf.Name()), zipFilePath)
			if err != nil {
				logger.Errorf("Backup folder: %s couldn't be uploaded. Err: %v", humanReadableBackupFolder, err)
				logger.Debugf("Backup folder: %s couldn't be uploaded. Err: %v", fmt.Sprintf("%s/%s", bf.Name(), uf.Name()), err)
				return nil
			}
			logger.Infof("Backup folder: %s were succesfully uploaded to %s", humanReadableBackupFolder, bucketURL)
			logger.Debugf("Backup folder: %s were succesfully uploaded to %s", fmt.Sprintf("%s/%s", bf.Name(), uf.Name()), bucketURL)
		}
	}

	// Remove all files and folder under backup folder.
	if err := util.RemoveAllContent(backupFolderPath); err != nil {
		logger.Errorf("Backup folders couldn't be cleaned up. Err: %v", err)
	}

	logger.Infof("Backup folders were succesfully cleaned up from local.")
	return nil
}

func uploadBackupToBucket(ctx context.Context, bucket *blob.Bucket, fileName string, filePath string) error {
	defer bucket.Close()
	if accessible, _ := bucket.IsAccessible(ctx); accessible {
		w, err := bucket.NewWriter(context.Background(), fileName, nil)
		if err != nil {
			return fmt.Errorf("Could not create writer: %v", err)
		}
		defer w.Close()

		src, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("Could not open file: %v", err)
		}

		_, err = io.Copy(w, src)
		if err != nil {
			return fmt.Errorf("Failed to copy data: %v\n", err)
		}
	} else {
		return fmt.Errorf("Bucket is not accesible.")
	}
	return nil
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
