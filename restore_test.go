package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/hazelcast/platform-operator-agent/backup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"
)

var exampleTarGzFiles = []file{
	{"cluster", true},
	{"cluster/cluster-state.txt", false},
	{"cluster/cluster-version.txt", false},
	{"cluster/partition-thread-count.bin", false},
	{"configs", true},
	{"s00", true},
	{"s00/tombstone", true},
	{"cluster/members.bin", false},
	{"s00/tombstone/02", true},
	{"s00/tombstone/02/0000000000000002.chunk", false},
	{"s00/value", true},
	{"s00/value/01", true},
	{"s00/value/01/0000000000000001.chunk", false},
}

func TestDownload(t *testing.T) {
	tests := []struct {
		name            string
		keys            []string
		uuids           []string
		id              int
		want            string
		wantDeletedUUID string
		wantErr         bool
	}{
		{"no .tar.gz in keys",
			[]string{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},
			[]string{"a", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},
			0, "", "", true},
		{"index out of range",
			[]string{"00000000-0000-0000-0000-000000000001.tar.gz", "00000000-0000-0000-0000-000000000002.tar.gz"},
			[]string{"a", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},
			2, "", "", true},
		{"mixed keys",
			[]string{"a.tar.gz", "b.tar", "c.tar.gz2", "d.tar.gz"},
			[]string{"a", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},
			1, "d.tar.gz", "00000000-0000-0000-0000-000000000002", false},
		{
			"no uuid folder",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
				"2006-01-02-15-04-01/b.tar.gz",
			},
			[]string{"a", "b", "c"},
			0,
			"2006-01-02-15-04-01/a.tar.gz",
			"",
			false,
		},
		{
			"one uuid folder, key and uuid are equal",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
				"2007-01-02-15-04-01/00000000-0000-0000-0000-000000000001.tar.gz",
			},
			[]string{"a", "00000000-0000-0000-0000-000000000001"},
			0,
			"2007-01-02-15-04-01/00000000-0000-0000-0000-000000000001.tar.gz",
			"",
			false,
		},
		{
			"one uuid folder, key and uuid are different",
			[]string{
				"2005-01-02-15-04-01/a.tar.gz",
				"2006-01-02-15-04-01/a.tar.gz",
				"2006-01-02-15-04-01/b.tar.gz",
			},
			[]string{"a", "00000000-0000-0000-0000-000000000001"},
			1,
			"2006-01-02-15-04-01/b.tar.gz",
			"00000000-0000-0000-0000-000000000001",
			false,
		},
		{
			"multiple uuid folders, key and uuid are equal",
			[]string{
				"2006-01-02-15-04-01/foo.tar.gz",
				"2006-01-02-15-04-02/foo.tar.gz",
				"2022-06-13-00-00-00/00000000-0000-0000-0000-000000000001.tar.gz",
				"2022-06-13-00-00-00/00000000-0000-0000-0000-000000000002.tar.gz",
				"2022-06-13-00-00-00/00000000-0000-0000-0000-000000000003",
				"2022-06-13-00-00-00/00000000-0000-0000-0000-000000000004.tar.gz",
			},
			[]string{"a", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002", "00000000-0000-0000-0000-000000000004"},
			2,
			"2022-06-13-00-00-00/00000000-0000-0000-0000-000000000004.tar.gz",
			"",
			false,
		},
		{
			"multiple uuid folders, key and uuid are different",
			[]string{
				"2006-01-02-15-04-01/foo.tar.gz",
				"2006-01-02-15-04-02/foo.tar.gz",
				"2022-06-13-00-00-00/a.tar.gz",
				"2022-06-13-00-00-00/b.tar",
				"2022-06-13-00-00-00/c.tar.gz",
				"2022-06-13-00-00-00/d.tar.gz",
			},
			[]string{"a", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002", "00000000-0000-0000-0000-000000000003"},
			1,
			"2022-06-13-00-00-00/c.tar.gz",
			"00000000-0000-0000-0000-000000000002",
			false,
		},
		{
			"multiple uuid folders, mismatching number of keys and uuids",
			[]string{
				"2006-01-02-15-04-01/foo.tar.gz",
				"2006-01-02-15-04-02/foo.tar.gz",
				"2022-06-13-00-00-00/a.tar.gz",
				"2022-06-13-00-00-00/b.tar.gz",
				"2022-06-13-00-00-00/c.tar.gz",
				"2022-06-13-00-00-00/d.tar.gz",
			},
			[]string{"a", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002", "00000000-0000-0000-0000-000000000003"},
			0,
			"",
			"",
			true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := ioutil.TempDir("", "")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			tarGzFilesBaseDir := path.Join(tmpdir, "archieve")
			err = createFiles(tarGzFilesBaseDir, exampleTarGzFiles, true)
			require.Nil(t, err)

			dstPath := path.Join(tmpdir, "dest")
			for _, uuid := range tt.uuids {
				err = os.MkdirAll(path.Join(dstPath, uuid), 0700)
				require.Nil(t, err)
			}

			wantUUID := strings.TrimSuffix(path.Base(tt.want), ".tar.gz")
			bucketPath := path.Join(tmpdir, "bucket")
			for _, key := range tt.keys {
				file := path.Join(bucketPath, key)
				uuid := strings.TrimSuffix(path.Base(key), ".tar.gz")
				err = createArchieveFile(tarGzFilesBaseDir, uuid, file)
				require.Nil(t, err)
			}

			bucket, err := fileblob.OpenBucket(bucketPath, nil)
			require.Nil(t, err)
			defer bucket.Close()

			// test
			err = download(ctx, "file://"+bucketPath, dstPath, tt.id, nil)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			if tt.want == "" {
				return
			}
			wantTarGzFileList, err := getDirFileList(tarGzFilesBaseDir)
			require.Nil(t, err)

			gotFileList, err := getDirFileList(path.Join(dstPath, wantUUID))
			require.Nil(t, err)

			require.ElementsMatch(t, wantTarGzFileList, gotFileList)

			if tt.wantDeletedUUID != "" {
				require.NoDirExists(t, path.Join(dstPath, tt.wantDeletedUUID))
			}
		})
	}

}
func TestFind(t *testing.T) {
	tests := []struct {
		name    string
		keys    []string
		want    []string
		wantErr bool
	}{
		{"extension", []string{"foo"}, nil, true},
		{"single", []string{"foo.tar.gz"}, []string{"foo.tar.gz"}, false},
		{"id", []string{"a.tar.gz", "b.tar.gz"}, []string{"a.tar.gz", "b.tar.gz"}, false},
		{
			"single with date",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
			},
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
			},
			false,
		},
		{
			"id with date",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
				"2006-01-02-15-04-01/b.tar.gz",
			},
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
				"2006-01-02-15-04-01/b.tar.gz",
			},
			false,
		},
		{
			"latest",
			[]string{
				"2006-01-02-15-04-01/foo.tar.gz",
				"2006-01-02-15-04-02/foo.tar.gz",
				"2022-06-13-00-00-00/foo.tar.gz",
			},
			[]string{
				"2022-06-13-00-00-00/foo.tar.gz",
			},
			false,
		},
		{
			"mixed",
			[]string{
				"bar.tar.gz",
				"foo/bar.tar.gz",
				"2006-01-02-15-04-01/foo.tar.gz",
			},
			[]string{
				"2006-01-02-15-04-01/foo.tar.gz",
			},
			false,
		},
		// {
		// 	"dates prefixed in different folders",
		// 	[]string{
		// 		"hz-cr1/2006-01-02-15-04-02/foo.tar.gz",
		// 		"hz-cr2/2006-01-02-15-04-01/foo.tar.gz",
		// 	},
		// 	nil,
		// 	false,
		// },
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			bucket := memblob.OpenBucket(nil)
			defer bucket.Close()
			for _, k := range tt.keys {
				err := bucket.WriteAll(ctx, k, []byte(""), nil)
				require.Nil(t, err)
			}

			// test
			got, err := find(ctx, bucket)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSaveFromArchieve(t *testing.T) {
	tests := []struct {
		name    string
		files   []file
		wantErr bool
	}{
		{
			"example", []file{
				{name: "file1"},
				{name: "folder2", isDir: true},
				{name: "folder2/file6.xy"},
				{name: "folder2/folder4", isDir: true},
				{name: "folder2/folder4/file2.xyz"},
			}, false,
		},
		{
			"empty folder", []file{}, false,
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := ioutil.TempDir("", "save_from_tar_gzip")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			tarFilesDir := path.Join(tmpdir, "tarBaseDir")
			tarName := "dest.tar.gz"
			tarFilePath := path.Join(tmpdir, tarName)

			err = createFiles(tarFilesDir, tt.files, true)
			require.Nil(t, err)

			err = createArchieveFile(tarFilesDir, path.Base(tarFilesDir), tarFilePath)
			require.Nil(t, err)

			bucket, err := fileblob.OpenBucket(path.Dir(tarFilePath), nil)
			require.Nil(t, err)

			// Run test
			destDir := path.Join(tmpdir, "dest")
			require.Nil(t, err)

			err = saveFromArchieve(ctx, bucket, tarName, destDir)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			gotFiles, err := getDirFileList(path.Join(destDir, "tarBaseDir"))
			require.Nil(t, err)
			require.ElementsMatch(t, tt.files, gotFiles)

		})
	}
}

func TestParseID(t *testing.T) {
	tests := []struct {
		name     string
		hostName string
		want     int
		wantErr  bool
	}{
		{
			"incorrect", "hazelcast", 0, true,
		},
		{
			"correct 1", "hazelcast-134", 134, false,
		},
		{
			"correct 2", "aa-10-11-12-13", 13, false,
		},
		{
			"correct 3", "1-2-3-4", 4, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i, err := parseID(tt.hostName)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.want, i)
		})
	}
}

func createArchieveFile(dir, baseDir, outPath string) error {
	err := os.MkdirAll(path.Dir(outPath), 0700)
	if err != nil {
		return err
	}
	outFile, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer outFile.Close()

	return backup.CreateArchieve(outFile, dir, baseDir)
}
