package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
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

func UploadBackup(ctx context.Context, bucket *blob.Bucket, backupsDir, prefix string) error {
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

		seq, err := convertHumanReadableFormat(s.Name())
		if err != nil {
			return err
		}

		// iterate over <backup-dir>/backup-<backupSeq>/<UUID> dirs
		for _, u := range backupUUIDs {
			uuidDir := filepath.Join(seqDir, u.Name())
			key := filepath.Join(prefix, seq, u.Name()+".tar.gz")

			err := func() error {
				defer os.RemoveAll(uuidDir)
				return uploadBackup(ctx, bucket, key, uuidDir, u.Name())
			}()
			if err != nil {
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
	log.Println("Uploading", backupDir, name)
	w, err := bucket.NewWriter(ctx, name, nil)
	if err != nil {
		return err
	}
	defer w.Close()

	return CreateArchieve(w, backupDir, baseDir)
}

func CreateArchieve(w io.Writer, dir, baseDir string) error {
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

		// make sure files are relative to baseDir
		header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, dir))

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
