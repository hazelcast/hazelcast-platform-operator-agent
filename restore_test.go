package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
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
		name    string
		keys    []string
		id      int
		want    string
		wantErr bool
	}{
		{"extension", []string{"foo"}, 0, "", false},
		{"single", []string{"foo.tar.gz"}, 0, "foo.tar.gz", false},
		{"id", []string{"a.tar.gz", "b.tar.gz"}, 1, "b.tar.gz", false},
		{"overflow", []string{"foo.tar.gz"}, 99, "", false},
		{
			"single with date",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
			},
			0,
			"2006-01-02-15-04-01/a.tar.gz",
			false,
		},
		{
			"id with date",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
				"2006-01-02-15-04-01/b.tar.gz",
			},
			1,
			"2006-01-02-15-04-01/b.tar.gz",
			false,
		},
		{
			"latest",
			[]string{
				"2006-01-02-15-04-01/foo.tar.gz",
				"2006-01-02-15-04-02/foo.tar.gz",
				"2022-06-13-00-00-00/foo.tar.gz",
			},
			0,
			"2022-06-13-00-00-00/foo.tar.gz",
			false,
		},
		{
			"mixed",
			[]string{
				"bar.tar.gz",
				"foo/bar.tar.gz",
				"2006-01-02-15-04-01/foo.tar.gz",
			},
			0,
			"2006-01-02-15-04-01/foo.tar.gz",
			false,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := ioutil.TempDir("", "download")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			uuid := "52cea3e3-7f6a-411f-8ab4-cb207c4d0f55"
			tarGzFilesBaseDir := path.Join(tmpdir, uuid)

			err = createFiles(tarGzFilesBaseDir, exampleTarGzFiles)
			require.Nil(t, err)

			bucketPath := path.Join(tmpdir, "bucket")
			for _, key := range tt.keys {
				file := path.Join(bucketPath, key)
				if key == tt.want {
					err = createArchieveFile(tarGzFilesBaseDir, uuid, file)
					require.Nil(t, err)
					continue
				}
				_, err = createFile(file)
				require.Nil(t, err)
			}

			bucket, err := fileblob.OpenBucket(bucketPath, nil)
			require.Nil(t, err)
			defer bucket.Close()

			// test
			dstPath := path.Join(tmpdir, "dest")
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

			gotFileList, err := getDirFileList(path.Join(dstPath, uuid))
			require.Nil(t, err)

			require.ElementsMatch(t, wantTarGzFileList, gotFileList)

		})
	}

}
func TestFind(t *testing.T) {
	tests := []struct {
		name    string
		keys    []string
		id      int
		want    string
		wantErr bool
	}{
		{"extension", []string{"foo"}, 0, "", false},
		{"single", []string{"foo.tar.gz"}, 0, "foo.tar.gz", false},
		{"id", []string{"a.tar.gz", "b.tar.gz"}, 1, "b.tar.gz", false},
		{"overflow", []string{"foo.tar.gz"}, 99, "", false},
		{
			"single with date",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
			},
			0,
			"2006-01-02-15-04-01/a.tar.gz",
			false,
		},
		{
			"id with date",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
				"2006-01-02-15-04-01/b.tar.gz",
			},
			1,
			"2006-01-02-15-04-01/b.tar.gz",
			false,
		},
		{
			"latest",
			[]string{
				"2006-01-02-15-04-01/foo.tar.gz",
				"2006-01-02-15-04-02/foo.tar.gz",
				"2022-06-13-00-00-00/foo.tar.gz",
			},
			0,
			"2022-06-13-00-00-00/foo.tar.gz",
			false,
		},
		{
			"mixed",
			[]string{
				"bar.tar.gz",
				"foo/bar.tar.gz",
				"2006-01-02-15-04-01/foo.tar.gz",
			},
			0,
			"2006-01-02-15-04-01/foo.tar.gz",
			false,
		},
		// {
		// 	"top",
		// 	[]string{
		// 		"foo/2006-01-02-15-04-02/foo.tar.gz",
		// 		"bar/2006-01-02-15-04-01/foo.tar.gz",
		// 	},
		// 	0,
		// 	"",
		// 	true,
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
			got, err := find(ctx, bucket, tt.id)
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

			err = createFiles(tarFilesDir, tt.files)
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

func createFile(filePath string) (*os.File, error) {
	err := os.MkdirAll(path.Dir(filePath), 0700)
	if err != nil {
		return nil, err
	}

	return os.Create(filePath)
}
