package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"gocloud.dev/blob"

	"github.com/hazelcast/platform-operator-agent/bucket"
)

type customClassDownloadCmd struct {
	Bucket      string `envconfig:"CCD_BUCKET"`
	Destination string `envconfig:"CCD_DESTINATION"`
	SecretName  string `envconfig:"CCD_SECRET_NAME"`
}

func (*customClassDownloadCmd) Name() string     { return "custom-class-download" }
func (*customClassDownloadCmd) Synopsis() string { return "Run Custom Class Download Agent" }
func (*customClassDownloadCmd) Usage() string    { return "" }

func (r *customClassDownloadCmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	f.StringVar(&r.Bucket, "src", "", "src bucket path")
	f.StringVar(&r.Destination, "dst", "/opt/hazelcast/customClass", "dst filesystem path")
	f.StringVar(&r.SecretName, "secret-name", "", "secret name for the bucket credentials")
}

func (r *customClassDownloadCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Println("Starting ccd agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("ccd", r); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	bucketURI, err := formatURI(r.Bucket)
	if err != nil {
		return subcommands.ExitFailure
	}

	secretData, err := bucket.GetSecretData(ctx, r.SecretName)
	if err != nil {
		log.Println("error fetching secret data", err)
		return subcommands.ExitFailure
	}

	// run download process
	if err := downloadClassJars(ctx, bucketURI, r.Destination, secretData); err != nil {
		log.Println("download error", err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

func downloadClassJars(ctx context.Context, src, dst string, secretData map[string][]byte) error {
	bucket, err := bucket.OpenBucket(ctx, src, secretData)
	if err != nil {
		return err
	}
	defer bucket.Close()

	iter := bucket.List(nil)
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

		if err := saveJar(ctx, bucket, obj.Key, dst); err != nil {
			return err
		}
	}

	return nil
}

func saveJar(ctx context.Context, bucket *blob.Bucket, key, path string) error {
	s, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return err
	}
	defer s.Close()

	destPath := filepath.Join(path, key)

	d, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer d.Close()

	if _, err := io.Copy(d, s); err != nil {
		return err
	}

	// flush file
	if err := d.Sync(); err != nil {
		return err
	}

	return nil
}
