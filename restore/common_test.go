package restore

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
)

func TestSaveFromArchive(t *testing.T) {
	tests := []struct {
		name    string
		files   []fileutil.File
		wantErr bool
	}{
		{
			"example", []fileutil.File{
				{Name: "file1"},
				{Name: "folder2", IsDir: true},
				{Name: "folder2/file6.xy"},
				{Name: "folder2/folder4", IsDir: true},
				{Name: "folder2/folder4/file2.xyz"},
			}, false,
		},
		{
			"empty folder", []fileutil.File{}, false,
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := os.MkdirTemp("", "save_from_tar_gzip")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			tarFilesDir := path.Join(tmpdir, "tarBaseDir")
			tarName := "dest.tar.gz"
			tarFilePath := path.Join(tmpdir, tarName)

			err = fileutil.CreateFiles(tarFilesDir, tt.files, true)
			require.Nil(t, err)

			err = createArchiveFile(tarFilesDir, path.Base(tarFilesDir), tarFilePath)
			require.Nil(t, err)

			bucket, err := fileblob.OpenBucket(path.Dir(tarFilePath), nil)
			require.Nil(t, err)

			// Run test
			destDir := path.Join(tmpdir, "dest")
			require.Nil(t, err)

			err = saveFromArchive(ctx, bucket, tarName, destDir)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			gotFiles, err := fileutil.DirFileList(path.Join(destDir, "tarBaseDir"))
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
