package backup

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob"

	"backup-agent/util"
)

func UploadBackup(ctx context.Context, bucketURL string, backupFolderPath string, hazelcastCRName string) error {
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
			uploadErr := uploadBackupToBucket(ctx, bucketURL, fmt.Sprintf("%s/%s/%s.zip", hazelcastCRName, humanReadableBackupFolder, uf.Name()), zipFilePath)
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

func uploadBackupToBucket(ctx context.Context, bucketURL string, fileName string, filePath string) error {
	bucket, err := blob.OpenBucket(context.Background(), bucketURL)
	if err != nil {
		return fmt.Errorf("Could not open %s bucket %v", bucketURL, err)
	}
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
