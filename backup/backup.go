package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"

	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/s3blob"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
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
	g, ctx := errgroup.WithContext(ctx)
	for _, s := range backupSeqs {
		seqDir := filepath.Join(backupsDir, s.Name())
		backupUUIDs, err := ioutil.ReadDir(seqDir)
		if err != nil {
			return err
		}

		seq := convertHumanReadableFormat(s.Name())

		// iterate over <backup-dir>/backup-<backupSeq>/<UUID> dirs
		for _, u := range backupUUIDs {
			ID := u.Name()
			backupDir := filepath.Join(seqDir, ID)
			backupKey := filepath.Join(prefix, seq, ID+".tar.gz")
			g.Go(func() error {
				// always remove backup dir to reclaim space
				defer os.RemoveAll(backupDir)

				// run upload concurrently
				return uploadBackup(ctx, bucket, backupKey, backupDir, ID)
			})
		}

		if err := g.Wait(); err != nil {
			return err
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

// convertHumanReadableFormat converts backup-sequenceID into human readable format.
// backup-1643801670242 --> 2022-02-18-14-57-44
func convertHumanReadableFormat(backupFolderName string) string {
	epochString := strings.ReplaceAll(backupFolderName, "backup-", "")
	timestamp, _ := strconv.ParseInt(epochString, 10, 64)
	t := time.UnixMilli(timestamp)
	return t.Format("2006-01-02-15-04-05")
}
