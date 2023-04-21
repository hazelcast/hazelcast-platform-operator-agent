package bucket

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
)

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
			err = SaveFileFromBucket(context.Background(), b, tt.key, dstPath)
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
			err = DownloadClassJars(context.Background(), "file://"+bucketPath, dstPath, nil)
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

func TestDownloadFile(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "download_files")
	require.Nil(t, err)
	defer os.RemoveAll(tmpdir)

	bucketPath := path.Join(tmpdir, "bucket")
	files := []fileutil.File{{Name: "file1.jar"}, {Name: "file2.jar"}, {Name: "jar3.jar"}}
	err = fileutil.CreateFiles(bucketPath, files, true)
	require.Nil(t, err)

	dstPath, err := os.MkdirTemp(tmpdir, "dest")
	require.Nil(t, err, "Destination Path could not be created")

	err = DownloadFile(context.Background(), "file://"+bucketPath, dstPath, "file2.jar", nil)
	require.Nil(t, err, "Error downloading file")
	copiedFiles, err := fileutil.DirFileList(dstPath)
	require.Nil(t, err)
	require.ElementsMatch(t, copiedFiles, []fileutil.File{{Name: "file2.jar"}})
}
