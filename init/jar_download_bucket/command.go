package downloadbucket

import (
	"context"
	"flag"
	"os"
	"path/filepath"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"

	"github.com/hazelcast/platform-operator-agent/internal/bucket"
	"github.com/hazelcast/platform-operator-agent/internal/logger"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
)

const bucketLock = ".download_bucket"

var log = logger.New().Named("download_bucket")

type Cmd struct {
	Destination string `envconfig:"JDB_DESTINATION" yaml:"destination"`
	SecretName  string `envconfig:"JDB_SECRET_NAME" yaml:"secretName"`
	BucketURI   string `envconfig:"JDB_BUCKET_URI" yaml:"bucketURI"`
}

func (*Cmd) Name() string     { return "jar-download-bucket" }
func (*Cmd) Synopsis() string { return "Run Download Buckets agent" }
func (*Cmd) Usage() string    { return "" }

func (r *Cmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	f.StringVar(&r.BucketURI, "src", "", "src bucket path")
	f.StringVar(&r.Destination, "dst", "", "dst filesystem path")
	f.StringVar(&r.SecretName, "secret-name", "", "secret name for the bucket credentials")
}

func (r *Cmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Info("starting download bucket agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("jdb", r); err != nil {
		log.Error("an error occurred while processing config from env: " + err.Error())
		return subcommands.ExitFailure
	}

	lock := filepath.Join(r.Destination, bucketLock)
	if _, err := os.Stat(lock); err == nil || os.IsExist(err) {
		// If usercodeLock lock exists exit
		log.Error("lock file exists, exiting: " + err.Error())
		return subcommands.ExitSuccess
	}

	bucketURI, err := uri.NormalizeURI(r.BucketURI)
	if err != nil {
		log.Error("an error occurred while normalizing URI: " + err.Error())
		return subcommands.ExitFailure
	}
	log.Info("bucket URI normalized successfully", zap.String("bucket URI", bucketURI))

	// run download process
	log.Info("starting download", zap.String("destination", r.Destination))
	if err = bucket.DownloadFiles(ctx, bucketURI, r.Destination, r.SecretName); err != nil {
		log.Error("download error: " + err.Error())
		return subcommands.ExitFailure
	}

	if err = os.WriteFile(lock, []byte{}, 0600); err != nil {
		log.Error("lock file creation error: " + err.Error())
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}
