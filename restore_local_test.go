package main

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopyBackup(t *testing.T) {
	tests := []struct {
		name        string
		memberID    int
		keys        []file
		destUUIDs   []file
		want        string
		wantDeleted string
		wantErr     bool
	}{
		{
			"empty backup dir",
			0,
			[]file{},
			[]file{},
			"",
			"",
			true,
		},
		{
			"single backup",
			0,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
			},
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			"",
			false,
		},
		{
			"incorrect member id but isolated backups",
			5,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
			},
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			"",
			false,
		},
		{
			"single backup, backup and hot-restart uuids are different",
			0,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
			},
			[]file{
				{name: "00000000-0000-0000-0000-00000000000a", isDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			"00000000-0000-0000-0000-00000000000a",
			false,
		},
		{
			"member ID is out of index error",
			2,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "00000000-0000-0000-0000-000000000002", isDir: true},
			},
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "00000000-0000-0000-0000-000000000002", isDir: true},
			},
			"",
			"",
			true,
		},
		{
			"mismatching number of backup folders and dest backup folders",
			0,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "00000000-0000-0000-0000-000000000002", isDir: true},
			},
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
			},
			"",
			"",
			true,
		},
		{
			"multiple backups ",
			2,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "00000000-0000-0000-0000-000000000002", isDir: true},
				{name: "00000000-0000-0000-0000-000000000003", isDir: true},
				{name: "00000000-0000-0000-0000-000000000004", isDir: true},
			},
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "00000000-0000-0000-0000-000000000002", isDir: true},
				{name: "00000000-0000-0000-0000-000000000003", isDir: true},
				{name: "00000000-0000-0000-0000-000000000004", isDir: true},
			},
			"00000000-0000-0000-0000-000000000003",
			"",
			false,
		},
		{
			"multiple backups, backup and hot-restart uuids are different",
			2,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "00000000-0000-0000-0000-000000000002", isDir: true},
				{name: "00000000-0000-0000-0000-000000000003", isDir: true},
				{name: "00000000-0000-0000-0000-000000000004", isDir: true},
			},
			[]file{
				{name: "00000000-0000-0000-0000-00000000000a", isDir: true},
				{name: "00000000-0000-0000-0000-00000000000b", isDir: true},
				{name: "00000000-0000-0000-0000-00000000000c", isDir: true},
				{name: "00000000-0000-0000-0000-00000000000d", isDir: true},
			},
			"00000000-0000-0000-0000-000000000003",
			"00000000-0000-0000-0000-00000000000c",
			false,
		},
		{
			"multiple backups with incorrect backup name and file type",
			1,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "000000000000", isDir: true},
				{name: "00000000-0000-0000-0000-000000000003", isDir: false},
				{name: "00000000-0000-0000-0000-000000000004", isDir: true},
				{name: "abc", isDir: true},
			},
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "00000000-0000-0000-0000-00000000000a", isDir: true},
			},
			"00000000-0000-0000-0000-000000000004",
			"00000000-0000-0000-0000-00000000000a",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := os.MkdirTemp("", "upload_backup")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			// create backupDir and add backup contents
			backupDir, err := os.MkdirTemp(tmpdir, "backupDir")
			require.Nil(t, err)
			err = createFiles(backupDir, tt.keys, true)
			require.Nil(t, err)

			// create backupDir and add backup contents
			destDir, err := os.MkdirTemp(tmpdir, "destDir")
			require.Nil(t, err)
			err = createFiles(destDir, tt.destUUIDs, true)
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
