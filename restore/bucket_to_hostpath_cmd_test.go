package restore

import (
	"context"
	"github.com/hazelcast/platform-operator-agent/internal"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
	"os"
	"path"
	"strings"
	"testing"
)

func TestDownloadToHostpath(t *testing.T) {
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
			tmpdir, err := os.MkdirTemp("", "")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			tarGzFilesBaseDir := path.Join(tmpdir, "archive")
			err = internal.CreateFiles(tarGzFilesBaseDir, ExampleTarGzFiles, true)
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
			err = downloadToHostpath(ctx, "file://"+bucketPath, dstPath, tt.id, nil)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			if tt.want == "" {
				return
			}
			wantTarGzFileList, err := internal.DirFileList(tarGzFilesBaseDir)
			require.Nil(t, err)

			gotFileList, err := internal.DirFileList(path.Join(dstPath, wantUUID))
			require.Nil(t, err)

			require.ElementsMatch(t, wantTarGzFileList, gotFileList)

			if tt.wantDeletedUUID != "" {
				require.NoDirExists(t, path.Join(dstPath, tt.wantDeletedUUID))
			}
		})
	}
}
