package restore

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/hazelcast/platform-operator-agent/init/bucket"
	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
	"github.com/hazelcast/platform-operator-agent/internal/logger"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
)

const restoreLock = "restore_lock"

var (
	// StatefulSet hostname is always DSN RFC 1123 and number
	hostnameRE = regexp.MustCompile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?-([0-9]+)$")

	// Backup directory name is a formated date e.g. 2006-01-02-15-04-05/
	dateRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}/`)

	// lock file, e.g. .restore_lock.12345.12
	lockRE = regexp.MustCompile(`^\.` + restoreLock + `\.[a-z0-9]*\.\d*$`)

	bucketToHostpathLog = logger.New().Named("restore_from_bucket_to_hostpath")
)

type BucketToHostpathCmd struct {
	Bucket      string `envconfig:"RESTORE_BUCKET"`
	Destination string `envconfig:"RESTORE_DESTINATION"`
	Hostname    string `envconfig:"RESTORE_HOSTNAME"`
	SecretName  string `envconfig:"RESTORE_SECRET_NAME"`
	RestoreID   string `envconfig:"RESTORE_ID"`
}

func (*BucketToHostpathCmd) Name() string     { return "restore_hostpath" }
func (*BucketToHostpathCmd) Synopsis() string { return "run restore_hostpath agent" }
func (*BucketToHostpathCmd) Usage() string    { return "" }

func (r *BucketToHostpathCmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	hostname, _ := os.Hostname()
	f.StringVar(&r.Hostname, "hostname", hostname, "dst filesystem path")
	f.StringVar(&r.Bucket, "src", "", "src bucket path")
	f.StringVar(&r.Destination, "dst", "/data/persistence/backup", "dst filesystem path")
	f.StringVar(&r.SecretName, "secret-name", "", "secret name for the bucket credentials")
}

func (r *BucketToHostpathCmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	bucketToHostpathLog.Info("Starting restore agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("restore", r); err != nil {
		bucketToHostpathLog.Error("an error occurred while processing config from env: " + err.Error())
		return subcommands.ExitFailure
	}

	if !hostnameRE.MatchString(r.Hostname) {
		bucketToHostpathLog.Error("invalid hostname, need to conform to statefulset naming scheme")
		return subcommands.ExitFailure
	}

	id, err := parseID(r.Hostname)
	if err != nil {
		return subcommands.ExitFailure
	}
	bucketToHostpathLog.Info("agent id parsed successfully", zap.Int("agent id", id))

	bucketURI, err := uri.NormalizeURI(r.Bucket)
	if err != nil {
		return subcommands.ExitFailure
	}
	bucketToHostpathLog.Info("bucket URI normalized successfully", zap.String("bucket URI", bucketURI))

	lock := filepath.Join(r.Destination, lockFileName(r.RestoreID, id))

	if _, err = os.Stat(lock); err == nil || os.IsExist(err) {
		// If restore lock exists exit
		bucketToHostpathLog.Info("restore lock exists, exiting")
		return subcommands.ExitSuccess
	}

	bucketToHostpathLog.Info("reading secret", zap.String("secret name", r.SecretName))
	secretData, err := bucket.SecretData(ctx, r.SecretName)
	if err != nil {
		bucketToHostpathLog.Error("error fetching secret data: " + err.Error())
		return subcommands.ExitFailure
	}

	// run download process
	bucketToHostpathLog.Info("starting download:", zap.String("destination", r.Destination), zap.Int("id", id))
	if err = downloadToHostpath(ctx, bucketURI, r.Destination, id, secretData); err != nil {
		bucketToHostpathLog.Error("download error: " + err.Error())
		return subcommands.ExitFailure
	}

	if err = cleanupLocks(r.Destination, id); err != nil {
		bucketToHostpathLog.Error("Error cleaning up locks: " + err.Error())
		return subcommands.ExitFailure
	}

	if err = os.WriteFile(lock, []byte{}, 0600); err != nil {
		bucketToHostpathLog.Error("lock file creation error: " + err.Error())
		return subcommands.ExitFailure
	}

	bucketToHostpathLog.Info("restore successful")
	return subcommands.ExitSuccess
}

func downloadToHostpath(ctx context.Context, src, dst string, id int, secretData map[string][]byte) error {
	b, err := bucket.OpenBucket(ctx, src, secretData)
	if err != nil {
		return err
	}
	defer b.Close()

	// find keys, they are sorted
	keys, err := find(ctx, b)
	if err != nil {
		return err
	}

	if id >= len(keys) {
		return fmt.Errorf("Member index %d is greater than number of archived backup files %d", id, len(keys))
	}

	// find backup UUIDs, they are sorted
	hotRestartUUIDs, err := fileutil.FolderUUIDs(dst)
	if err != nil {
		return err
	}

	var key string
	var uuidToDelete string

	switch lenUUIDs := len(hotRestartUUIDs); {
	case lenUUIDs == 0:
		key = keys[id]
	case lenUUIDs == 1:
		uuidToDelete = hotRestartUUIDs[0].Name()
		// try to match the existing hot-restart folder with the backup folder
		for _, bkey := range keys {
			if strings.TrimSuffix(path.Base(bkey), ".tar.gz") == uuidToDelete {
				key = bkey
				break
			}
		}
		// Assume user wants to restore from a completely different cluster
		if key == "" {
			bucketToHostpathLog.Info("restored backup UUID is different from the local hot-restart folder UUID!")
			key = keys[id]
		}
	// If there are multiple backups, members are not isolated
	case lenUUIDs > 1:
		if lenUUIDs != len(keys) {
			return fmt.Errorf("mismatching local hot-restart folder count %d and archived backup file count %d", lenUUIDs, len(keys))
		}
		if strings.TrimSuffix(path.Base(keys[id]), ".tar.gz") != hotRestartUUIDs[id].Name() {
			// Assume user wants to restore from a completely different cluster
			bucketToHostpathLog.Info("restored backup UUID is different from the local hot-restart folder UUID!")
		}
		key = keys[id]
		uuidToDelete = hotRestartUUIDs[id].Name()
	}

	// cleanup hot-restart folder if present
	if uuidToDelete != "" {
		bucketToHostpathLog.Info("deleting the hot-restart folder: ", zap.String("uuids", uuidToDelete))
		if err = os.RemoveAll(path.Join(dst, uuidToDelete)); err != nil {
			return err
		}
	}

	bucketToHostpathLog.Info("restoring", zap.String("key", key))
	if err = saveFromArchive(ctx, b, key, dst); err != nil {
		return err
	}

	return nil
}
