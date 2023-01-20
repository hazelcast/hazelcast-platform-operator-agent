package restore

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
)

func TestCopyBackup(t *testing.T) {
	tests := []struct {
		Name        string
		memberID    int
		keys        []fileutil.File
		destUUIDs   []fileutil.File
		want        string
		wantDeleted string
		wantErr     bool
	}{
		{
			"empty backup dir",
			0,
			[]fileutil.File{},
			[]fileutil.File{},
			"",
			"",
			true,
		},
		{
			"single backup",
			0,
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			"",
			false,
		},
		{
			"incorrect member id but isolated backups",
			5,
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			"",
			false,
		},
		{
			"single backup, backup and hot-restart uuids are different",
			0,
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-00000000000a", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			"00000000-0000-0000-0000-00000000000a",
			false,
		},
		{
			"member ID is out of index error",
			2,
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
			},
			"",
			"",
			true,
		},
		{
			"mismatching number of backup folders and dest backup folders",
			0,
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
			},
			"",
			"",
			true,
		},
		{
			"multiple backups",
			2,
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000003", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000004", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000003", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000004", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000003",
			"",
			false,
		},
		{
			"multiple backups, backup and hot-restart uuids are different",
			2,
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000002", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000003", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000004", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-00000000000a", IsDir: true},
				{Name: "00000000-0000-0000-0000-00000000000b", IsDir: true},
				{Name: "00000000-0000-0000-0000-00000000000c", IsDir: true},
				{Name: "00000000-0000-0000-0000-00000000000d", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000003",
			"00000000-0000-0000-0000-00000000000c",
			false,
		},
		{
			"multiple backups with incorrect backup Name and file type",
			1,
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "000000000000", IsDir: true},
				{Name: "00000000-0000-0000-0000-000000000003", IsDir: false},
				{Name: "00000000-0000-0000-0000-000000000004", IsDir: true},
				{Name: "abc", IsDir: true},
			},
			[]fileutil.File{
				{Name: "00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "00000000-0000-0000-0000-00000000000a", IsDir: true},
			},
			"00000000-0000-0000-0000-000000000004",
			"00000000-0000-0000-0000-00000000000a",
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
			err = copyBackup(backupDir, destDir, tt.memberID)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			require.DirExists(t, path.Join(destDir, tt.want))
			if tt.wantDeleted != "" {
				require.NoDirExists(t, path.Join(destDir, tt.wantDeleted))
			}

		})
	}
}
