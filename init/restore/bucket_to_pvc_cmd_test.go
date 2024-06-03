package restore

import (
	"context"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
)

var exampleTarGzFiles = []fileutil.File{
	{Name: "cluster", IsDir: true},
	{Name: "cluster/cluster-state.txt", IsDir: false},
	{Name: "cluster/cluster-version.txt", IsDir: false},
	{Name: "cluster/partition-thread-count.bin", IsDir: false},
	{Name: "configs", IsDir: true},
	{Name: "s00", IsDir: true},
	{Name: "s00/tombstone", IsDir: true},
	{Name: "cluster/members.bin", IsDir: false},
	{Name: "s00/tombstone/02", IsDir: true},
	{Name: "s00/tombstone/02/0000000000000002.chunk", IsDir: false},
	{Name: "s00/value", IsDir: true},
	{Name: "s00/value/01", IsDir: true},
	{Name: "s00/value/01/0000000000000001.chunk", IsDir: false},
}

func TestDownloadFromBucketToPVC(t *testing.T) {
	tests := []struct {
		name    string
		keys    []string
		uuids   []string
		id      int
		want    string
		wantErr bool
	}{
		{"no .tar.gz in keys",
			[]string{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},
			[]string{"a", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},
			0,
			"",
			true,
		},
		{"index out of range",
			[]string{"00000000-0000-0000-0000-000000000001.tar.gz", "00000000-0000-0000-0000-000000000002.tar.gz"},
			[]string{"a", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},
			2,
			"",
			true,
		},
		{"mixed keys",
			[]string{
				"00000000-0000-0000-0000-000000000001",
				"00000000-0000-0000-0000-000000000002.tar",
				"00000000-0000-0000-0000-000000000003.tar.gz",
				"00000000-0000-0000-0000-000000000004.tar.gz",
			},
			[]string{
				"00000000-0000-0000-0000-000000000003",
				"00000000-0000-0000-0000-000000000004",
			},
			1, "00000000-0000-0000-0000-000000000004.tar.gz", false},
		{
			"no uuid folder",
			[]string{
				"2006-01-02-15-04-01/00000000-0000-0000-0000-000000000001.tar.gz",
				"2006-01-02-15-04-01/00000000-0000-0000-0000-000000000002.tar.gz",
			},
			[]string{
				"00000000-0000-0000-0000-000000000001",
				"00000000-0000-0000-0000-000000000002",
			},
			0,
			"2006-01-02-15-04-01/00000000-0000-0000-0000-000000000001.tar.gz",
			false,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := os.MkdirTemp("", "")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			tarGzFilesBaseDir := path.Join(tmpdir, "archive")
			err = fileutil.CreateFiles(tarGzFilesBaseDir, exampleTarGzFiles, true)
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
				err = createArchiveFile(tarGzFilesBaseDir, uuid, file)
				require.Nil(t, err)
			}

			bucket, err := fileblob.OpenBucket(bucketPath, nil)
			require.Nil(t, err)
			defer bucket.Close()

			// test

			err = downloadFromBucketToPvc(ctx, "file://"+bucketPath, dstPath, tt.id, "")
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			if tt.want == "" {
				return
			}
			wantTarGzFileList, err := fileutil.DirFileList(tarGzFilesBaseDir)
			require.Nil(t, err)

			gotFileList, err := fileutil.DirFileList(path.Join(dstPath, wantUUID))
			require.Nil(t, err)

			require.ElementsMatch(t, wantTarGzFileList, gotFileList)

			f, err := fileutil.FolderUUIDs(dstPath)
			require.Nil(t, err)
			require.Equal(t, len(f), 1)
			require.Equal(t, f[0].Name(), wantUUID)
		})
	}
}
