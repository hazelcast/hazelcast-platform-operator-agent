package usercode_bucket

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/fileblob"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
)

func TestDownloadClassJars(t *testing.T) {
	tests := []struct {
		name          string
		dstPathExists bool
		files         []fileutil.File
		wantFiles     []fileutil.File
		wantErr       bool
	}{
		{
			"only jar allowed",
			true,
			[]fileutil.File{
				{Name: "file1"},
				{Name: "test1.jar"},
				{Name: "test2.class"},
			},
			[]fileutil.File{
				{Name: "test1.jar"},
			},
			false,
		},
		{
			"no subfolder jars allowed",
			true,
			[]fileutil.File{
				{Name: "folder1/test2.jar"},
				{Name: "test1.jar"},
				{Name: "test2.jar"},
			},
			[]fileutil.File{
				{Name: "test1.jar"},
				{Name: "test2.jar"},
			},
			false,
		},
		{
			"no jar",
			true,
			[]fileutil.File{
				{Name: "folder1/test2.jar"},
				{Name: "test1.jar2"},
				{Name: "jarjar"},
			},
			[]fileutil.File{},
			false,
		},
		{
			"dest path does not exist",
			false,
			[]fileutil.File{
				{Name: "test1.jar"},
			},
			[]fileutil.File{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare the buckets and destination folder
			tmpdir, err := os.MkdirTemp("", "download_class_jars")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			bucketPath := path.Join(tmpdir, "bucket")
			err = fileutil.CreateFiles(bucketPath, tt.files, true)
			require.Nil(t, err)

			var dstPath string
			if tt.dstPathExists {
				dstPath, err = os.MkdirTemp(tmpdir, "dest")
				require.Nil(t, err, "Destination Path could not be created")
			} else {
				dstPath = path.Join(tmpdir, "dest-does-not-exist")
			}

			// Run the tests
			err = downloadClassJars(context.Background(), "file://"+bucketPath, dstPath, nil)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				require.Contains(t, err.Error(), "no such file or directory")
				return
			}
			copiedFiles, err := fileutil.DirFileList(dstPath)
			require.Nil(t, err)
			require.ElementsMatch(t, copiedFiles, tt.wantFiles)
		})
	}
}
