package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"gocloud.dev/blob"

	_ "gocloud.dev/blob/s3blob"
)

// StatefullSet hostname is always DSN RFC 1123 and number
var hostnameRe = regexp.MustCompile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?-([0-9]+)$")

type restoreCmd struct {
	Bucket      string `envconfig:"RESTORE_BUCKET"`
	Destination string `envconfig:"RESTORE_DESTINATION"`
	Hostname    string `envconfig:"RESTORE_HOSTNAME"`
}

func (*restoreCmd) Name() string     { return "restore" }
func (*restoreCmd) Synopsis() string { return "run restore agent" }
func (*restoreCmd) Usage() string    { return "" }

func (r *restoreCmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	hostname, _ := os.Hostname()
	f.StringVar(&r.Hostname, "hostname", hostname, "dst filesystem path")
	f.StringVar(&r.Bucket, "src", "", "src bucket path")
	f.StringVar(&r.Destination, "dst", "/data/persistence/backup", "dst filesystem path")
}

func (r *restoreCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// overwrite config with environment variables
	if err := envconfig.Process("restore", r); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	if !hostnameRe.MatchString(r.Hostname) {
		log.Println("Invalid hostname, need to confrom to statefullset naming scheme")
		return subcommands.ExitFailure
	}

	id, err := parseID(r.Hostname)
	if err != nil {
		return subcommands.ExitFailure
	}

	// run download process
	if err := download(ctx, r.Bucket, r.Destination, id); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

func download(ctx context.Context, src, dst string, id int) error {
	bucket, err := blob.OpenBucket(ctx, src)
	if err != nil {
		return err
	}
	defer bucket.Close()

	var keys []string

	iter := bucket.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// naive validation, we only want zip files
		if !strings.HasSuffix(obj.Key, ".zip") {
			continue
		}

		keys = append(keys, obj.Key)
	}

	if len(keys) == 0 || id > len(keys)-1 {
		// skip download
		return nil
	}

	// to be extra safe we always sort the keys
	sort.Strings(keys)

	if err := save(ctx, bucket, keys[id], dst); err != nil {
		return err
	}

	return nil
}

func save(ctx context.Context, bucket *blob.Bucket, key, path string) error {
	s, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return err
	}
	defer s.Close()

	d, err := os.Create(filepath.Join(path, key))
	if err != nil {
		return err
	}
	defer d.Close()

	if _, err := io.Copy(d, s); err != nil {
		return err
	}

	return nil
}

var errParseID = errors.New("Couldn't parse statefullset hostname")

func parseID(hostname string) (int, error) {
	parts := hostnameRe.FindAllStringSubmatch(hostname, -1)
	if len(parts) != 1 && len(parts[0]) != 3 {
		return 0, errParseID
	}
	return strconv.Atoi(parts[0][2])
}
