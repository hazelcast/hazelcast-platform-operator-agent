package backup

import (
	"context"
	"fmt"

	"io"
	"io/ioutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"os"
	"path"
	"strings"

	_ "gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"gocloud.dev/blob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"

	"github.com/hazelcast/platform-operator-agent/util"
)

func UploadBackup(ctx context.Context, bucket *blob.Bucket, bucketURL, backupFolderPath, hazelcastCRName string) error {
	backupFolderFileList, backupFolderErr := ioutil.ReadDir(backupFolderPath)
	logger := util.GetLogger(ctx)
	if backupFolderErr != nil {
		logger.Errorf("Error occurred while read backup folder: %v", backupFolderErr)
		return nil
	}

	if len(backupFolderFileList) == 0 {
		logger.Printf("Backup folder doesn't have files/folders. Backup folder: %s", backupFolderFileList)
		return nil
	}

	logger.Info("Uploading backup folders to the bucket...")

	for _, bf := range backupFolderFileList {
		if bf.Name() == util.BucketDataGCPCredentialFile {
			continue
		}
		backupItem := fmt.Sprintf("%s/%s", backupFolderPath, bf.Name())
		UUIDFolderList, UUIDFolderErr := ioutil.ReadDir(backupItem)
		if UUIDFolderErr != nil {
			logger.Errorf("Error occurred while read backup folder for timestamp: %v", UUIDFolderErr)
			return nil
		}
		for _, uf := range UUIDFolderList {
			zipFilePath := fmt.Sprintf("/%s/%s.zip", backupFolderPath, uf.Name())
			UUIDBackupFolderPath := path.Join(backupFolderPath, bf.Name(), uf.Name())
			backupZipErr := util.ZipFolder(UUIDBackupFolderPath, zipFilePath)
			if backupZipErr != nil {
				logger.Errorf("Couldn't zip hot backup folder: %s. Err: %v", UUIDBackupFolderPath, backupZipErr)
				return nil
			}
			humanReadableBackupFolder := util.ConvertHumanReadableFormat(bf.Name())
			uploadErr := uploadBackupToBucket(ctx, bucket, fmt.Sprintf("%s/%s/%s.zip", hazelcastCRName, humanReadableBackupFolder, uf.Name()), zipFilePath)
			if uploadErr != nil {
				logger.Errorf("Backup folder: %s couldn't be uploaded. Err: %v", humanReadableBackupFolder, uploadErr)
				logger.Debugf("Backup folder: %s couldn't be uploaded. Err: %v", fmt.Sprintf("%s/%s", bf.Name(), uf.Name()), uploadErr)
				return nil
			}
			logger.Infof("Backup folder: %s were succesfully uploaded to %s", humanReadableBackupFolder, bucketURL)
			logger.Debugf("Backup folder: %s were succesfully uploaded to %s", fmt.Sprintf("%s/%s", bf.Name(), uf.Name()), bucketURL)
		}
	}
	// Remove all files and folder under backup folder.
	removeErr := util.RemoveAllContent(backupFolderPath)
	if removeErr != nil {
		logger.Errorf("Backup folders couldn't be cleaned up. Err: %v", removeErr)
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

func GetBucket(ctx context.Context, bucketURL, secretName string) (*blob.Bucket, error) {
	provider := strings.Split(bucketURL, ":")[0]
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	secret, err := clientset.CoreV1().Secrets("default").Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	switch provider {
	case util.AWS:
		if err := setCredentialEnv(secret.Data, util.BucketDataS3AccessKeyID, util.BucketDataS3EnvAccessKeyID, provider); err != nil {
			return nil, err
		}
		if err := setCredentialEnv(secret.Data, util.BucketDataS3Region, util.BucketDataS3EnvRegion, provider); err != nil {
			return nil, err
		}
		if err := setCredentialEnv(secret.Data, util.BucketDataS3SecretAccessKey, util.BucketDataS3EnvSecretAccessKey, provider); err != nil {
			return nil, err
		}
		bucket, err := blob.OpenBucket(ctx, bucketURL)
		if err != nil {
			return nil, fmt.Errorf("could not open %s bucket %v", bucketURL, err)
		}
		return bucket, nil
	case util.GCP:
		credValue, ok := secret.Data[util.BucketDataGCPCredentialFile]
		if !ok {
			return nil, fmt.Errorf("invalid secret for %v : missing credential: %v", provider, util.BucketDataGCPCredentialFile)
		}
		creds, err := google.CredentialsFromJSON(ctx, credValue, "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return nil, err
		}
		client, err := gcp.NewHTTPClient(
			gcp.DefaultTransport(),
			gcp.CredentialsTokenSource(creds))
		if err != nil {
			return nil, err
		}

		bucketName := strings.Split(bucketURL, ":")[1][2:]

		bucket, err := gcsblob.OpenBucket(ctx, client, bucketName, nil)
		if err != nil {
			return nil, fmt.Errorf("could not open %s bucket %v", bucketURL, err)
		}
		return bucket, nil
	case util.AZURE:
		if err := setCredentialEnv(secret.Data, util.BucketDataAzureStorageAccount, util.BucketDataAzureEnvStorageAccount, provider); err != nil {
			return nil, err
		}
		if err := setCredentialEnv(secret.Data, util.BucketDataAzureStorageKey, util.BucketDataAzureEnvStorageKey, provider); err != nil {
			return nil, err
		}
		bucket, err := blob.OpenBucket(ctx, bucketURL)
		if err != nil {
			return nil, fmt.Errorf("could not open %s bucket %v", bucketURL, err)
		}
		return bucket, nil
	default:
		return nil, fmt.Errorf("invalid bucket path")
	}
}

func setCredentialEnv(secretData map[string][]byte, credKey, credEnvKey, provider string) error {
	credValue, ok := secretData[credKey]
	if !ok {
		return fmt.Errorf("invalid secret for %v : missing credential: %v", provider, credKey)
	}
	os.Setenv(credEnvKey, string(credValue))
	return nil
}
