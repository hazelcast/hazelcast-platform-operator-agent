package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/subcommands"
	"github.com/hazelcast/platform-operator-agent/bucket"
	"github.com/kelseyhightower/envconfig"
	"gocloud.dev/blob"

	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

const restoreLock = ".restore_lock"

var (
	// StatefulSet hostname is always DSN RFC 1123 and number
	hostnameRE = regexp.MustCompile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?-([0-9]+)$")

	// Backup directory name is a formated date e.g. 2006-01-02-15-04-05/
	dateRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}/`)
)

type restoreCmd struct {
	Bucket      string `envconfig:"RESTORE_BUCKET"`
	Destination string `envconfig:"RESTORE_DESTINATION"`
	Hostname    string `envconfig:"RESTORE_HOSTNAME"`
	SecretName  string `envconfig:"RESTORE_SECRET_NAME"`
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
	f.StringVar(&r.SecretName, "secret-name", "", "secret name for the bucket credentials")
}

func (r *restoreCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
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

	bucketURI, err := formatURI(r.Bucket)
	if err != nil {
		return subcommands.ExitFailure
	}

	lock := filepath.Join(r.Destination, restoreLock)

	if _, err := os.Stat(lock); err == nil || os.IsExist(err) {
		// If restore lock exists exit
		log.Println("Restore lock exists, exiting")
		return subcommands.ExitSuccess
	}

	secretData, err := bucket.GetSecretData(ctx, r.SecretName)
	if err != nil {
		log.Println("error fetching secret data", err)
		return subcommands.ExitFailure
	}

	// run download process
	if err := download(ctx, bucketURI, r.Destination, id, secretData); err != nil {
		log.Println("download error", err)
		return subcommands.ExitFailure
	}

	if err := os.WriteFile(lock, []byte{}, 0600); err != nil {
		log.Println("Lock file creation error", err)
		return subcommands.ExitFailure
	}

	log.Println("Restore successful")
	return subcommands.ExitSuccess
}

func download(ctx context.Context, src, dst string, id int, secretData map[string][]byte) error {
	bucket, err := bucket.OpenBucket(ctx, src, secretData)
	if err != nil {
		return err
	}
	defer bucket.Close()

	key, err := find(ctx, bucket, id)
	if err != nil {
		return err
	}
	if key == "" {
		// skip download
		return nil
	}

	// cleanup hot-restart folder with the same id if present in the destination folder
	backupUUID := strings.TrimSuffix(path.Base(key), ".tar.gz")
	if err := os.RemoveAll(path.Join(dst, backupUUID)); err != nil {
		return err
	}

	log.Println("Restoring", key)
	if err := saveFromArchieve(ctx, bucket, key, dst); err != nil {
		return err
	}

	return nil
}

func find(ctx context.Context, bucket *blob.Bucket, id int) (string, error) {
	var keys []string
	var latest string
	iter := bucket.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		// naive validation, we only want tgz files
		if !strings.HasSuffix(obj.Key, ".tar.gz") {
			continue
		}

		// find latest directory if key starts with date (is in a directory with backups)
		if dateRE.MatchString(obj.Key) {
			dir := filepath.Dir(obj.Key)
			// lexicographical comparison is good enough
			if dir > latest {
				latest = dir
			}
		}

		keys = append(keys, obj.Key)
	}

	// this was a directory with backups, filter keys in latest backup
	if latest != "" {
		var l []string
		for _, k := range keys {
			if strings.HasPrefix(k, latest) {
				l = append(l, k)
			}
		}
		keys = l
	}

	if len(keys) == 0 || id > len(keys)-1 {
		// skip download
		return "", nil
	}

	// to be extra safe we always sort the keys
	sort.Strings(keys)

	return keys[id], nil
}

func saveFromArchieve(ctx context.Context, bucket *blob.Bucket, key, target string) error {
	s, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return err
	}
	defer s.Close()

	g, err := gzip.NewReader(s)
	if err != nil {
		return err
	}
	defer g.Close()

	t := tar.NewReader(g)
	for {
		header, err := t.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		name := filepath.Join(target, header.Name)
		if err := saveFile(name, header.FileInfo(), t); err != nil {
			return err
		}
	}
}

func saveFile(name string, info fs.FileInfo, src io.Reader) error {
	if info.IsDir() {
		return os.MkdirAll(name, info.Mode())
	}

	dst, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}

var errParseID = errors.New("Couldn't parse statefulset hostname")

func parseID(hostname string) (int, error) {
	parts := hostnameRE.FindAllStringSubmatch(hostname, -1)
	if parts == nil || (len(parts) != 1 && len(parts[0]) != 3) {
		return 0, errParseID
	}
	return strconv.Atoi(parts[0][2])
}

func removeAll(path string) error {
	names, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for _, e := range names {
		os.RemoveAll(filepath.Join(path, e.Name()))
	}
	return nil
}
