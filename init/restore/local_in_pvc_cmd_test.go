package restore

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
)

func TestCopyBackupPVC(t *testing.T) {

	tests := []struct {
		Name      string
		keys      []fileutil.File
		destUUIDs []fileutil.File
		want      string
		wantErr   bool
	}{
		{
			"empty backup dir",
			[]fileutil.File{},
			[]fileutil.File{},
			"",
			true,
		},
		{
			"single backup",
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			false,
		},
		{
			"incorrect member id but isolated backups",
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			false,
		},
		{
			"backup and hot-restart uuids are different",
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-00000000000a", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			false,
		},
		{
			"member ID is out of index error",
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
			},
			"",
			true,
		},
		{
			"multiple hot restart folders",
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000003", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000004", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000003",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			// Set up
			tmpdir, err := os.MkdirTemp("", "upload_backup")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			// create backupDir and add backup contents
			backupDir, err := os.MkdirTemp(tmpdir, "backupDir")
			require.Nil(t, err)
			err = fileutil.CreateFiles(backupDir, tt.keys, true)
			require.Nil(t, err)

			// create backupDir and add backup contents
			destDir, err := os.MkdirTemp(tmpdir, "destDir")
			require.Nil(t, err)
			err = fileutil.CreateFiles(destDir, tt.destUUIDs, true)
			require.Nil(t, err)

			//test
			err = copyBackupPVC(backupDir, destDir)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			require.DirExists(t, path.Join(destDir, tt.want))

			f, err := fileutil.FolderUUIDs(destDir)
			require.Nil(t, err)
			require.Equal(t, len(f), 1)
			require.Equal(t, f[0].Name(), tt.want)
		})
	}
}
