package sidecar

import (
	"context"
	"github.com/go-logr/logr"
	"log"
	"path"

	"github.com/google/uuid"

	"github.com/hazelcast/platform-operator-agent/init/bucket"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
)

// task is an upload process that is cancelable
type task struct {
	req       UploadReq
	ctx       context.Context
	cancel    context.CancelFunc
	backupKey string
	err       error
}

func (t *task) process(logger logr.Logger, ID uuid.UUID) {
	logger.Info("task is started", "task id", ID.ID())

	defer logger.Info("task is finished", "task id", ID.ID())
	defer t.cancel()

	bucketURI, err := uri.NormalizeURI(t.req.BucketURL)
	if err != nil {
		logger.Error(err, "error occurred while parsing bucket URI", "task id", ID)
		t.err = err
		return
	}

	logger.Info("bucket URI successfully normalized", "bucket URI", bucketURI)

	secretData, err := bucket.SecretData(t.ctx, t.req.SecretName)
	if err != nil {
		logger.Error(err, "error occurred while fetching secret", "task ID", ID)
		t.err = err
		return
	}

	logger.Info("task successfully read secret", "task id", ID, "secret name", t.req.SecretName)

	b, err := bucket.OpenBucket(t.ctx, bucketURI, secretData)
	if err != nil {
		logger.Error(err, "task could not open bucket", "task id", ID)
		t.err = err
		return
	}

	backupsDir := path.Join(t.req.BackupBaseDir, DirName)

	log.Println("TASK", ID, "Staring backup upload:", backupsDir, t.req.MemberID)
	folderKey, err := UploadBackup(t.ctx, b, backupsDir, t.req.HazelcastCRName, t.req.MemberID)
	if err != nil {
		logger.Error(err, "task could not upload to bucket", "task id", ID)
		t.err = err
		return
	}

	logger.Info("task finished upload", "task id", ID)

	backupKey, err := uri.AddFolderKeyToURI(bucketURI, folderKey)
	if err != nil {
		logger.Error(err, "task could not upload backup", "task id", ID)
		t.err = err
		return
	}

	t.backupKey = backupKey
}
