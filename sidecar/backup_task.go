package sidecar

import (
	"context"
	"log"
	"path"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gocloud.dev/blob"

	"github.com/hazelcast/platform-operator-agent/internal/bucket"
	"github.com/hazelcast/platform-operator-agent/internal/logger"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
)

var backupLog = logger.New().Named("backup")

// task is an upload process that is cancelable
type task struct {
	req       UploadReq
	ctx       context.Context
	cancel    context.CancelFunc
	backupKey string
	err       error
}

func (t *task) process(ID uuid.UUID) {
	backupLog.Info("task is started", zap.Uint32("task id", ID.ID()))

	defer backupLog.Info("task is finished", zap.Uint32("task id", ID.ID()))
	defer t.cancel()

	bucketURI, err := uri.NormalizeURI(t.req.BucketURL)
	if err != nil {
		backupLog.Error("error occurred while parsing bucket URI: "+err.Error(), zap.Uint32("task id", ID.ID()))
		t.err = err
		return
	}

	backupLog.Info("bucket URI successfully normalized", zap.String("bucket URI", bucketURI))

	secretData, err := bucket.SecretData(t.ctx, t.req.SecretName)
	if err != nil {
		backupLog.Error("error occurred while fetching secret: "+err.Error(), zap.Uint32("task ID", ID.ID()))
		t.err = err
		return
	}

	backupLog.Info("task successfully read secret", zap.Uint32("task id", ID.ID()), zap.String("secret name", t.req.SecretName))

	b, err := bucket.OpenBucket(t.ctx, bucketURI, secretData)
	if err != nil {
		backupLog.Error("task could not open bucket: "+err.Error(), zap.Uint32("task id", ID.ID()))
		t.err = err
		return
	}

	backupsDir := path.Join(t.req.BackupBaseDir, DirName)

	log.Println("TASK", ID, "Staring backup upload:", backupsDir, t.req.MemberID)
	folderKey, err := UploadBackup(t.ctx, b, backupsDir, t.req.HazelcastCRName, t.req.MemberID)
	if err != nil {
		backupLog.Error("task could not upload to bucket: "+err.Error(), zap.Uint32("task id", ID.ID()))
		t.err = err
		return
	}

	backupLog.Info("task finished upload", zap.Uint32("task id", ID.ID()))

	backupKey, err := uri.AddFolderKeyToURI(bucketURI, folderKey)
	if err != nil {
		backupLog.Error("task could not upload backup: "+err.Error(), zap.Uint32("task id", ID.ID()))
		t.err = err
		return
	}

	t.backupKey = backupKey
}

func (t *task) deleteFromBucket(ID uuid.UUID) {
	//ID to string
	backupLog.Info("task is deleting from bucket", zap.Uint32("task id", ID.ID()))

	defer backupLog.Info("task finished deleting from bucket", zap.Uint32("task id", ID.ID()))

	bucketURI, err := uri.NormalizeURI(t.req.BucketURL)
	if err != nil {
		backupLog.Error("error occurred while parsing bucket URI: "+err.Error(), zap.Uint32("task id", ID.ID()))
		t.err = err
		return
	}

	backupLog.Info("bucket URI successfully normalized", zap.String("bucket URI", bucketURI))

	secretData, err := bucket.SecretData(t.ctx, t.req.SecretName)
	if err != nil {
		backupLog.Error("error occurred while fetching secret: "+err.Error(), zap.Uint32("task ID", ID.ID()))
		t.err = err
		return
	}

	backupLog.Info("task successfully read secret", zap.Uint32("task id", ID.ID()), zap.String("secret name", t.req.SecretName))

	b, err := bucket.OpenBucket(t.ctx, bucketURI, secretData)
	if err != nil {
		backupLog.Error("task could not open bucket: "+err.Error(), zap.Uint32("task id", ID.ID()))
		t.err = err
		return
	}

	if err := DeleteBackup(t.ctx, b); err != nil {
		backupLog.Error("task could not delete from bucket: "+err.Error(), zap.Uint32("task id", ID.ID()))
		t.err = err
		return
	}
}

func DeleteBackup(ctx context.Context, b *blob.Bucket) error {
	return b.Delete(ctx, DirName)
}
