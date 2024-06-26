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

var log = logger.New().Named("restore_from_bucket_to_pvc")

type BucketToPVCCmd struct {
	Bucket      string `envconfig:"RESTORE_BUCKET" yaml:"bucket"`
	Destination string `envconfig:"RESTORE_DESTINATION" yaml:"destination"`
	SecretName  string `envconfig:"RESTORE_SECRET_NAME" yaml:"secretName"`
	RestoreID   string `envconfig:"RESTORE_ID" yaml:"restoreID"`
}

func (*BucketToPVCCmd) Name() string     { return "restore_pvc" }
func (*BucketToPVCCmd) Synopsis() string { return "run restore_pvc agent" }
func (*BucketToPVCCmd) Usage() string    { return "" }

func (r *BucketToPVCCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&r.Bucket, "src", "", "src bucket path")
	f.StringVar(&r.Destination, "dst", "/data/persistence/backup", "dst filesystem path")
	f.StringVar(&r.SecretName, "secret-name", "", "secret name for the bucket credentials")
}

func (r *BucketToPVCCmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Info("starting restore agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("restore", r); err != nil {
		log.Error("an error occurred while processing config from env: " + err.Error())
		return subcommands.ExitFailure
	}

	hostname := os.Getenv("HOSTNAME")
	if !hostnameRE.MatchString(hostname) {
		log.Error("invalid hostname, need to conform to statefulset naming scheme")
		return subcommands.ExitFailure
	}

	id, err := parseID(hostname)
	if err != nil {
		return subcommands.ExitFailure
	}
	log.Info("agent id parse successfully", zap.Int("agent id", id))

	bucketURI, err := uri.NormalizeURI(r.Bucket)
	if err != nil {
		return subcommands.ExitFailure
	}
	log.Info("bucket uri normalized successfully", zap.String("bucket URI", bucketURI))

	lock := filepath.Join(r.Destination, lockFileName(r.RestoreID, id))

	if _, err = os.Stat(lock); err == nil || os.IsExist(err) {
		// If restore lock exists exit
		log.Info("restore lock exists, exiting")
		return subcommands.ExitSuccess
	}

	// run download process
	log.Info("Starting download:", zap.Int(r.Destination, id))
	if err = downloadFromBucketToPvc(ctx, bucketURI, r.Destination, id, r.SecretName); err != nil {
		log.Error("download error: " + err.Error())
		return subcommands.ExitFailure
	}

	if err = cleanupLocks(r.Destination, id); err != nil {
		log.Error("error cleaning up locks: " + err.Error())
		return subcommands.ExitFailure
	}

	if err = os.WriteFile(lock, []byte{}, 0600); err != nil {
		log.Error("lock file creation error: " + err.Error())
		return subcommands.ExitFailure
	}

	log.Info("restore successful")
	return subcommands.ExitSuccess
}

func downloadFromBucketToPvc(ctx context.Context, src, dst string, id int, secretName string) error {
	b, err := bucket.OpenBucket(ctx, src, secretName)
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

	if _, err = os.Stat(dst); os.IsNotExist(err) {
		err = os.Mkdir(dst, 0755)
		if err != nil {
			return err
		}
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

	log.Info("restoring ", zap.String("key", keys[id]))
	if err = saveFromArchive(ctx, b, keys[id], dst); err != nil {
		return err
	}

	return b.Close()
}
