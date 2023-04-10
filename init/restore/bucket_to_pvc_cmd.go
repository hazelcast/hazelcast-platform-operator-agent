package restore

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"

	"github.com/hazelcast/platform-operator-agent/internal/bucket"
	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
	"github.com/hazelcast/platform-operator-agent/internal/logger"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
)

var bucketToPVCLog = logger.New().Named("restore_from_bucket_to_pvc")

type BucketToPVCCmd struct {
	Bucket      string `envconfig:"RESTORE_BUCKET"`
	Destination string `envconfig:"RESTORE_DESTINATION"`
	Hostname    string `envconfig:"RESTORE_HOSTNAME"`
	SecretName  string `envconfig:"RESTORE_SECRET_NAME"`
	RestoreID   string `envconfig:"RESTORE_ID"`
}

func (*BucketToPVCCmd) Name() string     { return "restore_pvc" }
func (*BucketToPVCCmd) Synopsis() string { return "run restore_pvc agent" }
func (*BucketToPVCCmd) Usage() string    { return "" }

func (r *BucketToPVCCmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	hostname, _ := os.Hostname()
	f.StringVar(&r.Hostname, "hostname", hostname, "dst filesystem path")
	f.StringVar(&r.Bucket, "src", "", "src bucket path")
	f.StringVar(&r.Destination, "dst", "/data/persistence/backup", "dst filesystem path")
	f.StringVar(&r.SecretName, "secret-name", "", "secret name for the bucket credentials")
}

func (r *BucketToPVCCmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	bucketToPVCLog.Info("starting restore agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("restore", r); err != nil {
		bucketToPVCLog.Error("an error occurred while processing config from env: " + err.Error())
		return subcommands.ExitFailure
	}

	if !hostnameRE.MatchString(r.Hostname) {
		bucketToPVCLog.Error("invalid hostname, need to conform to statefulset naming scheme")
		return subcommands.ExitFailure
	}

	id, err := parseID(r.Hostname)
	if err != nil {
		return subcommands.ExitFailure
	}
	bucketToPVCLog.Info("agent id parse successfully", zap.Int("agent id", id))

	bucketURI, err := uri.NormalizeURI(r.Bucket)
	if err != nil {
		return subcommands.ExitFailure
	}
	bucketToPVCLog.Info("bucket uri normalized successfully", zap.String("bucket URI", bucketURI))

	lock := filepath.Join(r.Destination, lockFileName(r.RestoreID, id))

	if _, err = os.Stat(lock); err == nil || os.IsExist(err) {
		// If restore lock exists exit
		bucketToPVCLog.Info("restore lock exists, exiting")
		return subcommands.ExitSuccess
	}

	bucketToPVCLog.Info("reading secret", zap.String("secret name", r.SecretName))
	secretData, err := bucket.SecretData(ctx, r.SecretName)
	if err != nil {
		bucketToPVCLog.Error("error fetching secret data: " + err.Error())
		return subcommands.ExitFailure
	}

	// run download process
	bucketToPVCLog.Info("Starting download:", zap.Int(r.Destination, id))
	if err = downloadFromBucketToPvc(ctx, bucketURI, r.Destination, id, secretData); err != nil {
		bucketToPVCLog.Error("download error: " + err.Error())
		return subcommands.ExitFailure
	}

	if err = cleanupLocks(r.Destination, id); err != nil {
		bucketToPVCLog.Error("error cleaning up locks: " + err.Error())
		return subcommands.ExitFailure
	}

	if err = os.WriteFile(lock, []byte{}, 0600); err != nil {
		bucketToPVCLog.Error("lock file creation error: " + err.Error())
		return subcommands.ExitFailure
	}

	bucketToPVCLog.Info("restore successful")
	return subcommands.ExitSuccess
}

func downloadFromBucketToPvc(ctx context.Context, src, dst string, id int, secretData map[string][]byte) error {
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
		return fmt.Errorf("member index %d is greater than number of archived backup files %d", id, len(keys))
	}

	// find backup UUIDs, they are sorted
	hotRestartUUIDs, err := fileutil.FolderUUIDs(dst)
	if err != nil {
		return err
	}

	// Remove the hot-restart folder at the destination
	for _, uuid := range hotRestartUUIDs {
		err = os.RemoveAll(path.Join(dst, uuid.Name()))
		if err != nil {
			return err
		}
	}

	bucketToPVCLog.Info("restoring ", zap.String("key", keys[id]))
	if err = saveFromArchive(ctx, b, keys[id], dst); err != nil {
		return err
	}

	return nil
}
