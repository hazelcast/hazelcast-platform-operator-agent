package bucket

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
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
