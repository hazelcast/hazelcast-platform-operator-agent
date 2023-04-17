package downloadbucket

import (
	"context"
	"flag"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"

	"github.com/hazelcast/platform-operator-agent/internal/bucket"
	"github.com/hazelcast/platform-operator-agent/internal/logger"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
)

const bucketJarLock = ".jar_download_bucket"

var log = logger.New().Named("jar_download_bucket")

type Cmd struct {
	Destination string `envconfig:"JDB_DESTINATION"`
	SecretName  string `envconfig:"JDB_SECRET_NAME"`
	BucketURL   string `envconfig:"JDB_URL"`
}

func (*Cmd) Name() string     { return "jar-download-bucket" }
func (*Cmd) Synopsis() string { return "Run Jar Download Bucket agent" }
func (*Cmd) Usage() string    { return "" }

func (r *Cmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	f.StringVar(&r.BucketURL, "src", "", "src bucket path")
	f.StringVar(&r.Destination, "dst", "", "dst filesystem path")
	f.StringVar(&r.SecretName, "secret-name", "", "secret name for the bucket credentials")
}

func (r *Cmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Info("starting jar download bucket agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("jdb", r); err != nil {
		log.Error("an error occurred while processing config from env: " + err.Error())
		return subcommands.ExitFailure
	}

	lock := filepath.Join(r.Destination, bucketJarLock)
	if _, err := os.Stat(lock); err == nil || os.IsExist(err) {
		// If usercodeLock lock exists exit
		log.Error("lock file exists, exiting: " + err.Error())
		return subcommands.ExitSuccess
	}

	bucketURI, err := uri.NormalizeURI(r.BucketURL)
	if err != nil {
		return subcommands.ExitFailure
	}
	log.Info("bucket URI normalized successfully", zap.String("bucket URI", bucketURI))

	log.Info("reading secret", zap.String("secret name", r.SecretName))
	secretData, err := bucket.SecretData(ctx, r.SecretName)
	if err != nil {
		log.Error("error fetching secret data: " + err.Error())
		return subcommands.ExitFailure
	}

	// run download process
	log.Info("starting download", zap.String("destination", r.Destination))
	if err = downloadClassJars(ctx, bucketURI, r.Destination, secretData); err != nil {
		log.Error("download error: " + err.Error())
		return subcommands.ExitFailure
	}

	if err = os.WriteFile(lock, []byte{}, 0600); err != nil {
		log.Error("lock file creation error: " + err.Error())
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

func downloadClassJars(ctx context.Context, src, dst string, secretData map[string][]byte) error {
	b, err := bucket.OpenBucket(ctx, src, secretData)
	if err != nil {
		return err
	}
	defer b.Close()

	iter := b.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// naive validation, we only want jar files and no files under subfolders
		if !strings.HasSuffix(obj.Key, ".jar") || path.Base(obj.Key) != obj.Key {
			continue
		}

		if err = bucket.SaveFileFromBucket(ctx, b, obj.Key, dst); err != nil {
			return err
		}
	}

	return nil
}
