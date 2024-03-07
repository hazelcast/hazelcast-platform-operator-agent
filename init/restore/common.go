package restore

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"gocloud.dev/blob"

	"github.com/hazelcast/platform-operator-agent/sidecar"
)

func saveFromArchive(ctx context.Context, bucket *blob.Bucket, key, target string) error {
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
			break
		}
		if err != nil {
			return err
		}

		name := filepath.Join(target, header.Name)
		if err = saveFile(name, header.FileInfo(), t); err != nil {
			return err
		}
	}

	return s.Close()
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

func cleanupLocks(folder string, id int) error {
	locks, err := getLocks(folder)
	if err != nil {
		return err
	}

	for _, lock := range locks {
		if strings.HasSuffix(lock.Name(), "."+strconv.Itoa(id)) {
			err = os.Remove(path.Join(folder, lock.Name()))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getLocks(dir string) ([]os.DirEntry, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	locks := []os.DirEntry{}
	for _, file := range files {
		if lockRE.MatchString(file.Name()) {
			locks = append(locks, file)
		}
	}
	return locks, nil
}

func find(ctx context.Context, bucket *blob.Bucket) ([]string, error) {
	var keys []string
	var latest string
	iter := bucket.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// naive validation, we only want tgz files
		if !strings.HasSuffix(obj.Key, ".tar.gz") {
			continue
		}

		// find the latest directory if key starts with date (is in a directory with backups)
		if dateRE.MatchString(obj.Key) {
			dir := filepath.Dir(obj.Key)
			// lexicographical comparison is good enough
			if dir > latest {
				latest = dir
			}
		}

		keys = append(keys, obj.Key)
	}

	// this was a directory with backups, filter keys in the latest backup
	if latest != "" {
		var l []string
		for _, k := range keys {
			if strings.HasPrefix(k, latest) {
				l = append(l, k)
			}
		}
		keys = l
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("there are no archived backup files in the bucket")
	}

	// to be extra safe we always sort the keys
	sort.Strings(keys)

	return keys, nil
}

var errParseID = errors.New("couldn't parse statefulset hostname")

func parseID(hostname string) (int, error) {
	parts := hostnameRE.FindAllStringSubmatch(hostname, -1)
	if parts == nil || (len(parts) != 1 && len(parts[0]) != 3) {
		return 0, errParseID
	}
	return strconv.Atoi(parts[0][2])
}

func createArchiveFile(dir, baseDir, outPath string) error {
	err := os.MkdirAll(path.Dir(outPath), 0700)
	if err != nil {
		return err
	}
	outFile, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer outFile.Close()

	return sidecar.CreateArchive(outFile, dir, baseDir)
}
