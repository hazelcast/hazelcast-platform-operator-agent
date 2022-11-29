package restore

import (
	"github.com/hazelcast/platform-operator-agent/backup"
	"github.com/hazelcast/platform-operator-agent/internal"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"testing"
)

func TestCopyBackupPVC(t *testing.T) {

	tests := []struct {
		Name      string
		keys      []internal.File
		destUUIDs []internal.File
		want      string
		wantErr   bool
	}{
		{
			"empty backup dir",
			[]internal.File{},
			[]internal.File{},
			"",
			true,
		},
		{
			"single backup",
			[]internal.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			[]internal.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			false,
		},
		{
			"incorrect member id but isolated backups",
			[]internal.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			[]internal.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			false,
		},
		{
			"backup and hot-restart uuids are different",
			[]internal.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			[]internal.File{
				{Name: "00000000-0000-0000-0000-00000000000a", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			false,
		},
		{
			"member ID is out of index error",
			[]internal.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
			},
			[]internal.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
			},
			"",
			true,
		},
		{
			"multiple hot restart folders",
			[]internal.File{
				{Name: "00000000-0000-0000-0000-000000000003", IsDir: true},
			},
			[]internal.File{
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
			err = internal.CreateFiles(backupDir, tt.keys, true)
			require.Nil(t, err)

			// create backupDir and add backup contents
			destDir, err := os.MkdirTemp(tmpdir, "destDir")
			require.Nil(t, err)
			err = internal.CreateFiles(destDir, tt.destUUIDs, true)
			require.Nil(t, err)

			//test
			err = copyBackupPVC(backupDir, destDir)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			require.DirExists(t, path.Join(destDir, tt.want))

			f, err := backup.FolderUUIDs(destDir)
			require.Nil(t, err)
			require.Equal(t, len(f), 1)
			require.Equal(t, f[0].Name(), tt.want)
		})
	}
}
