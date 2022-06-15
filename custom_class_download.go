package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"gocloud.dev/blob"
)

type customClassDownloadCmd struct {
	Bucket      string `envconfig:"CCD_BUCKET"`
	Destination string `envconfig:"CCD_DESTINATION"`
}

func (*customClassDownloadCmd) Name() string     { return "custom-class-download" }
func (*customClassDownloadCmd) Synopsis() string { return "Run Custom Class Download Agent" }
func (*customClassDownloadCmd) Usage() string    { return "" }

func (r *customClassDownloadCmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	f.StringVar(&r.Bucket, "src", "", "src bucket path")
	f.StringVar(&r.Destination, "dst", "/opt/hazelcast/customClass", "dst filesystem path")
}

func (r *customClassDownloadCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Println("Starting ccd agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("ccd", r); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	// cleanup destination directory
	if err := removeAll(r.Destination); err != nil {
		log.Println("cleanup failed", err)
		return subcommands.ExitFailure
	}

	// run download process
	if err := downloadClassJars(ctx, r.Bucket, r.Destination); err != nil {
		log.Println("download error", err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

func downloadClassJars(ctx context.Context, src, dst string) error {
	bucket, err := blob.OpenBucket(ctx, src)
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
		// naive validation, we only want jar files
		if !strings.HasSuffix(obj.Key, ".jar") {
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

	dest_path := filepath.Join(path, key)

	d, err := os.Create(dest_path)
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

	if err := d.Close(); err != nil {
		return err
	}

	return nil
}
