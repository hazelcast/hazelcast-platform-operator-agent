package sidecar

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/s3blob"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
)

var (
	ErrEmptyBackupDir     = errors.New("empty backup directory")
	ErrMemberIDOutOfIndex = errors.New("MemberID is out of index for present backup folders")
)

func UploadBackup(ctx context.Context, bucket *blob.Bucket, backupsDir, prefix string, memberID int) (string, error) {
	backupSeqs, err := fileutil.FolderSequence(backupsDir)
	if err != nil {
		return "", err
	}

	if len(backupSeqs) == 0 {
		return "", ErrEmptyBackupDir
	}

	// Get the latest <backup-dir>/backup-<backupSeq> dir, ReadDir returns sorted slice
	latestSeq := backupSeqs[len(backupSeqs)-1]
	latestSeqDir := filepath.Join(backupsDir, latestSeq.Name())
	humanReadableSeq, err := convertHumanReadableFormat(latestSeq.Name())
	if err != nil {
		return "", err
	}

	backupUUIDS, err := fileutil.FolderUUIDs(latestSeqDir)
	if err != nil {
		return "", err
	}

	// If there are multiple backup UUIDs in the folder and memberID is out of index
	if len(backupUUIDS) != 1 && len(backupUUIDS) <= memberID {
		return "", ErrMemberIDOutOfIndex
	}

	// If there is only one backup, members are isolated. No need for memberID
	if len(backupUUIDS) == 1 {
		memberID = 0
	}
	uuid := backupUUIDS[memberID]
	uuidDir := filepath.Join(latestSeqDir, uuid.Name())
	key := filepath.Join(prefix, humanReadableSeq, uuid.Name()+".tar.gz")

	err = uploadBackup(ctx, bucket, key, uuidDir, uuid.Name())
	if err != nil {
		return "", err
	}

	err = os.WriteFile(uuidDir+".delete", []byte{}, 0600)
	if err != nil {
		return "", err
	}

	// we finished uploading backups, delete the sequence dir if all uuids are marked to be deleted
	if allFilesMarkedToBeDeleted(backupUUIDS, latestSeqDir) {
		os.RemoveAll(latestSeqDir)
	}

	return key, nil
}

func allFilesMarkedToBeDeleted(files []fs.DirEntry, dir string) bool {
	for _, file := range files {
		d := filepath.Join(dir, file.Name())
		if _, err := os.Stat(d + ".delete"); errors.Is(err, os.ErrNotExist) {
			return false
		}
	}
	return true
}

func uploadBackup(ctx context.Context, bucket *blob.Bucket, name, backupDir, baseDirName string) error {
	w, err := bucket.NewWriter(ctx, name, nil)
	if err != nil {
		return err
	}
	defer w.Close()

	if err := CreateArchive(w, backupDir, baseDirName); err != nil {
		return err
	}

	return w.Close()
}

func CreateArchive(w io.Writer, dir, baseDirName string) error {
	g := gzip.NewWriter(w)
	defer g.Close()

	t := tar.NewWriter(g)
	defer t.Close()

	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}

		// make sure files are relative to baseDirName
		header.Name = filepath.Join(baseDirName, strings.TrimPrefix(path, dir))

		if err = t.WriteHeader(header); err != nil {
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

// convertHumanReadableFormat converts backup-sequenceID into human-readable format.
// backup-1643801670242 --> 2022-02-18-14-57-44
func convertHumanReadableFormat(backupFolderName string) (string, error) {
	epochString := strings.ReplaceAll(backupFolderName, "backup-", "")
	timestamp, err := strconv.ParseInt(epochString, 10, 64)
	if err != nil {
		return "", err
	}
	t := time.UnixMilli(timestamp).UTC()
	return t.Format("2006-01-02-15-04-05"), nil
}
