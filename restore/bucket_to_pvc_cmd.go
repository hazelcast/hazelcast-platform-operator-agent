package restore

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/subcommands"
	"github.com/hazelcast/platform-operator-agent/backup"
	"github.com/hazelcast/platform-operator-agent/bucket"
	"github.com/hazelcast/platform-operator-agent/internal"
	"github.com/kelseyhightower/envconfig"
	"log"
	"os"
	"path"
	"path/filepath"
)

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

func (r *BucketToPVCCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Println("Starting restore agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("restore", r); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	if !hostnameRE.MatchString(r.Hostname) {
		log.Println("Invalid hostname, need to conform to statefulset naming scheme")
		return subcommands.ExitFailure
	}

	id, err := parseID(r.Hostname)
	if err != nil {
		return subcommands.ExitFailure
	}
	log.Println("Restore agent ID:", id)

	bucketURI, err := internal.FormatURI(r.Bucket)
	if err != nil {
		return subcommands.ExitFailure
	}
	log.Println("Bucket:", bucketURI)

	lock := filepath.Join(r.Destination, lockFileName(r.RestoreID, id))

	if _, err := os.Stat(lock); err == nil || os.IsExist(err) {
		// If restore lock exists exit
		log.Println("Restore lock exists, exiting")
		return subcommands.ExitSuccess
	}

	log.Println("Reading secret:", r.SecretName)
	secretData, err := bucket.SecretData(ctx, r.SecretName)
	if err != nil {
		log.Println("error fetching secret data", err)
		return subcommands.ExitFailure
	}

	// run download process
	log.Println("Starting download:", r.Destination, id)
	if err := downloadFromBucketToPvc(ctx, bucketURI, r.Destination, id, secretData); err != nil {
		log.Println("download error", err)
		return subcommands.ExitFailure
	}

	if err := cleanupLocks(r.Destination, id); err != nil {
		log.Println("Error cleaning up locks", err)
		return subcommands.ExitFailure
	}

	if err := os.WriteFile(lock, []byte{}, 0600); err != nil {
		log.Println("Lock file creation error", err)
		return subcommands.ExitFailure
	}

	log.Println("Restore successful")
	return subcommands.ExitSuccess
}

func downloadFromBucketToPvc(ctx context.Context, src, dst string, id int, secretData map[string][]byte) error {
	bucket, err := bucket.OpenBucket(ctx, src, secretData)
	if err != nil {
		return err
	}
	defer bucket.Close()

	// find keys, they are sorted
	keys, err := find(ctx, bucket)
	if err != nil {
		return err
	}

	if id >= len(keys) {
		return fmt.Errorf("member index %d is greater than number of archived backup files %d", id, len(keys))
	}

	// find backup UUIDs, they are sorted
	hotRestartUUIDs, err := backup.FolderUUIDs(dst)
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

	log.Println("Restoring", keys[id])
	if err = saveFromArchive(ctx, bucket, keys[id], dst); err != nil {
		return err
	}

	return nil
}
