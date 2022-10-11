package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/s3blob"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

var (
	BackupSequenceRegex = regexp.MustCompile(`^backup-\d{13}$`)
	BackupUUIDRegex     = regexp.MustCompile("^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$")

	ErrEmptyBackupDir     = errors.New("empty backup directory")
	ErrMemberIDOutOfIndex = errors.New("memberID is out of index for present backup folders")
)

func UploadBackup(ctx context.Context, bucket *blob.Bucket, backupsDir, prefix string, memberID int) (string, error) {
	backupSeqs, err := ioutil.ReadDir(backupsDir)
	if err != nil {
		return "", err
	}
	backupSeqs = FilterBackupSequenceFolders(backupSeqs)

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

	backupUUIDS, err := ioutil.ReadDir(latestSeqDir)
	if err != nil {
		return "", err
	}
	backupUUIDS = FilterBackupUUIDFolders(backupUUIDS)

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

	err = func() error {
		defer os.WriteFile(uuidDir+".delete", []byte{}, 0600)
		return uploadBackup(ctx, bucket, key, uuidDir, uuid.Name())
	}()
	if err != nil {
		return "", err
	}

	// we finished uploading backups, delete the sequence dir if all uuids are marked to be deleted
	if allFilesMarkedToBeDeleted(backupUUIDS, latestSeqDir) {
		os.RemoveAll(latestSeqDir)
	}

	return key, nil
}

func allFilesMarkedToBeDeleted(files []fs.FileInfo, dir string) bool {
	for _, file := range files {
		dir := filepath.Join(dir, file.Name())
		if _, err := os.Stat(dir + ".delete"); errors.Is(err, os.ErrNotExist) {
			return false
		}
	}
	return true
}

func uploadBackup(ctx context.Context, bucket *blob.Bucket, name, backupDir, baseDirName string) error {
	log.Println("Uploading", backupDir, name)
	w, err := bucket.NewWriter(ctx, name, nil)
	if err != nil {
		return err
	}
	defer w.Close()

	return CreateArchieve(w, backupDir, baseDirName)
}

func CreateArchieve(w io.Writer, dir, baseDirName string) error {
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
func convertHumanReadableFormat(backupFolderName string) (string, error) {
	epochString := strings.ReplaceAll(backupFolderName, "backup-", "")
	timestamp, err := strconv.ParseInt(epochString, 10, 64)
	if err != nil {
		return "", err
	}
	t := time.UnixMilli(timestamp).UTC()
	return t.Format("2006-01-02-15-04-05"), nil
}

func FilterBackupUUIDFolders(fs []os.FileInfo) []os.FileInfo {
	uuids := []os.FileInfo{}
	for _, f := range fs {
		if BackupUUIDRegex.MatchString(f.Name()) && f.IsDir() {
			uuids = append(uuids, f)
		}
	}
	return uuids
}

func FilterBackupSequenceFolders(fs []os.FileInfo) []os.FileInfo {
	seqs := []os.FileInfo{}
	for _, f := range fs {
		if BackupSequenceRegex.MatchString(f.Name()) && f.IsDir() {
			seqs = append(seqs, f)
		}
	}
	return seqs
}
