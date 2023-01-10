package sidecar

import (
	"context"
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

func (t *task) process(ID uuid.UUID) {
	log.Println("TASK", ID, "started")
	defer log.Printf("TASK %s finished: %+v", ID, t)
	defer t.cancel()

	bucketURI, err := uri.NormalizeURI(t.req.BucketURL)
	if err != nil {
		log.Println("TASK", ID, "Error occurred while parsing bucket URI:", err)
		t.err = err
		return
	}

	log.Println("TASK", ID, "Parsed bucketURI:", bucketURI)

	secretData, err := bucket.SecretData(t.ctx, t.req.SecretName)
	if err != nil {
		log.Println("TASK", ID, "Error occurred while fetching secret", err)
		t.err = err
		return
	}

	log.Println("TASK", ID, "Successfully read secret", t.req.SecretName)

	b, err := bucket.OpenBucket(t.ctx, bucketURI, secretData)
	if err != nil {
		log.Println("TASK", ID, "openBucket:", err)
		t.err = err
		return
	}

	backupsDir := path.Join(t.req.BackupBaseDir, DirName)

	log.Println("TASK", ID, "Staring backup upload:", backupsDir, t.req.MemberID)
	folderKey, err := UploadBackup(t.ctx, b, backupsDir, t.req.HazelcastCRName, t.req.MemberID)
	if err != nil {
		log.Println("TASK", ID, "uploadBackup:", err)
		t.err = err
		return
	}

	log.Println("TASK", ID, "Finished upload")

	backupKey, err := uri.AddFolderKeyToURI(bucketURI, folderKey)
	if err != nil {
		log.Println("TASK", ID, "uploadBackup:", err)
		t.err = err
		return
	}

	t.backupKey = backupKey
}
