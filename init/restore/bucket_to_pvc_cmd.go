package restore

import (
	"context"
	"flag"
	"fmt"
	"github.com/go-logr/logr"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"

	"github.com/hazelcast/platform-operator-agent/init/bucket"
	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
)

type BucketToPVCCmd struct {
	Bucket      string `envconfig:"RESTORE_BUCKET"`
	Destination string `envconfig:"RESTORE_DESTINATION"`
	Hostname    string `envconfig:"RESTORE_HOSTNAME"`
	SecretName  string `envconfig:"RESTORE_SECRET_NAME"`
	RestoreID   string `envconfig:"RESTORE_ID"`
	Logger      logr.Logger
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
	r.Logger.Info("starting restore agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("restore", r); err != nil {
		r.Logger.Error(err, "an error occurred while processing config from env")
		return subcommands.ExitFailure
	}

	if !hostnameRE.MatchString(r.Hostname) {
		r.Logger.Error(fmt.Errorf("need to conform to statefulset naming scheme"), "invalid hostname")
		return subcommands.ExitFailure
	}

	id, err := parseID(r.Hostname)
	if err != nil {
		return subcommands.ExitFailure
	}
	r.Logger.Info("agent id parse successfully", "agent id", id)

	bucketURI, err := uri.NormalizeURI(r.Bucket)
	if err != nil {
		return subcommands.ExitFailure
	}
	r.Logger.Info("bucket uri normalized successfully", "bucket URI", bucketURI)

	lock := filepath.Join(r.Destination, lockFileName(r.RestoreID, id))

	if _, err = os.Stat(lock); err == nil || os.IsExist(err) {
		// If restore lock exists exit
		r.Logger.Info("restore lock exists, exiting")
		return subcommands.ExitSuccess
	}

	r.Logger.Info("reading secret", "secret name", r.SecretName)
	secretData, err := bucket.SecretData(ctx, r.SecretName)
	if err != nil {
		r.Logger.Error(err, "error fetching secret data")
		return subcommands.ExitFailure
	}

	// run download process
	log.Println("Starting download:", r.Destination, id)
	if err = downloadFromBucketToPvc(ctx, r.Logger, bucketURI, r.Destination, id, secretData); err != nil {
		r.Logger.Error(err, "download error")
		return subcommands.ExitFailure
	}

	if err = cleanupLocks(r.Destination, id); err != nil {
		r.Logger.Error(err, "error cleaning up locks")
		return subcommands.ExitFailure
	}

	if err = os.WriteFile(lock, []byte{}, 0600); err != nil {
		r.Logger.Error(err, "lock file creation error")
		return subcommands.ExitFailure
	}

	r.Logger.Info("restore successful")
	return subcommands.ExitSuccess
}

func downloadFromBucketToPvc(ctx context.Context, logger logr.Logger, src, dst string, id int, secretData map[string][]byte) error {
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

	logger.Info("restoring ", "key", keys[id])
	if err = saveFromArchive(ctx, b, keys[id], dst); err != nil {
		return err
	}

	return nil
}
