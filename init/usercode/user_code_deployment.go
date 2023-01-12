package usercode

import (
	"context"
	"flag"
	"github.com/go-logr/logr"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"

	"github.com/hazelcast/platform-operator-agent/init/bucket"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
)

const usercodeLock = "usercode_lock"

type Cmd struct {
	Bucket      string `envconfig:"UCD_BUCKET"`
	Destination string `envconfig:"UCD_DESTINATION"`
	SecretName  string `envconfig:"UCD_SECRET_NAME"`
	Logger      logr.Logger
}

func (*Cmd) Name() string     { return "user-code-deployment" }
func (*Cmd) Synopsis() string { return "Run User Code Deployment Agent" }
func (*Cmd) Usage() string    { return "" }

func (r *Cmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	f.StringVar(&r.Bucket, "src", "", "src bucket path")
	f.StringVar(&r.Destination, "dst", "/opt/hazelcast/userCode/bucket", "dst filesystem path")
	f.StringVar(&r.SecretName, "secret-name", "", "secret name for the bucket credentials")
}

func (r *Cmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	r.Logger.Info("starting user code deployment agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("ucd", r); err != nil {
		r.Logger.Error(err, "an error occurred while processing config from env")
		return subcommands.ExitFailure
	}

	lock := filepath.Join(r.Destination, usercodeLock)
	if _, err := os.Stat(lock); err == nil || os.IsExist(err) {
		// If usercodeLock lock exists exit
		r.Logger.Error(err, "lock file exists, exiting")
		return subcommands.ExitSuccess
	}

	bucketURI, err := uri.NormalizeURI(r.Bucket)
	if err != nil {
		return subcommands.ExitFailure
	}
	r.Logger.Info("bucket URI normalized successfully", "bucket URI", bucketURI)

	r.Logger.Info("reading secret", "secret name", r.SecretName)
	secretData, err := bucket.SecretData(ctx, r.SecretName)
	if err != nil {
		r.Logger.Error(err, "error fetching secret data")
		return subcommands.ExitFailure
	}

	// run download process
	r.Logger.Info("starting download", "destination", r.Destination)
	if err = downloadClassJars(ctx, bucketURI, r.Destination, secretData); err != nil {
		r.Logger.Error(err, "download error")
		return subcommands.ExitFailure
	}

	if err = os.WriteFile(lock, []byte{}, 0600); err != nil {
		r.Logger.Error(err, "lock file creation error")
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
