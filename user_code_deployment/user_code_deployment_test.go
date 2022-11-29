package user_code_deployment

import (
	"context"
	"github.com/hazelcast/platform-operator-agent/bucket"
	"github.com/hazelcast/platform-operator-agent/internal"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"
)

func TestDownloadClassJars(t *testing.T) {
	tests := []struct {
		name          string
		dstPathExists bool
		files         []internal.File
		wantFiles     []internal.File
		wantErr       bool
	}{
		{
			"only jar allowed",
			true,
			[]internal.File{
				{Name: "file1"},
				{Name: "test1.jar"},
				{Name: "test2.class"},
			},
			[]internal.File{
				{Name: "test1.jar"},
			},
			false,
		},
		{
			"no subfolder jars allowed",
			true,
			[]internal.File{
				{Name: "folder1/test2.jar"},
				{Name: "test1.jar"},
				{Name: "test2.jar"},
			},
			[]internal.File{
				{Name: "test1.jar"},
				{Name: "test2.jar"},
			},
			false,
		},
		{
			"no jar",
			true,
			[]internal.File{
				{Name: "folder1/test2.jar"},
				{Name: "test1.jar2"},
				{Name: "jarjar"},
			},
			[]internal.File{},
			false,
		},
		{
			"dest path does not exist",
			false,
			[]internal.File{
				{Name: "test1.jar"},
			},
			[]internal.File{},
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
			err = internal.CreateFiles(bucketPath, tt.files, true)
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
			copiedFiles, err := internal.DirFileList(dstPath)
			require.Nil(t, err)
			require.ElementsMatch(t, copiedFiles, tt.wantFiles)
		})
	}
}
func TestSaveFileFromBackup(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		dstPathExists bool
		errWanted     bool
	}{
		{"file should be saved", "file1.txt", true, false},
		{"file in sub folder should not be saved", "folder1/file1.jar", true, true},
		{"dest path does not exist", "file1.jar", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			tmpdir, err := os.MkdirTemp("", "save_file_from_backup")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			b := memblob.OpenBucket(nil)
			defer b.Close()
			err = b.WriteAll(context.Background(), tt.key, []byte("content"), nil)
			require.Nil(t, err)

			var dstPath string
			if tt.dstPathExists {
				dstPath, err = os.MkdirTemp(tmpdir, "dest")
				require.Nil(t, err, "Destination Path could not be created")
			} else {
				dstPath = path.Join(tmpdir, "dest-does-not-exist")
			}

			// Run the tests
			err = bucket.SaveFileFromBucket(context.Background(), b, tt.key, dstPath)
			require.Equal(t, tt.errWanted, err != nil, "Error is: ", err)
			if err != nil {
				require.Contains(t, err.Error(), "no such file or directory")
				return
			}
			filePath := path.Join(dstPath, tt.key)
			file, err := os.ReadFile(filePath)
			require.Nil(t, err)
			require.Equal(t, []byte("content"), file)
		})
	}
}
