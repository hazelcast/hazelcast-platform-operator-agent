package main

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMoveBackup(t *testing.T) {
	tests := []struct {
		name     string
		memberID int
		keys     []file
		want     string
		wantErr  bool
	}{
		{
			"empty backup dir",
			0,
			[]file{},
			"",
			true,
		},
		{
			"single backup",
			0,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			false,
		},
		{
			"incorrect member id but isolated backups",
			5,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
			},
			"00000000-0000-0000-0000-000000000001",
			false,
		},
		{
			"member ID is out of index error",
			2,
			[]file{
				{name: "00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "00000000-0000-0000-0000-000000000002", isDir: true},
			},
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
			"00000000-0000-0000-0000-000000000003",
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
			"00000000-0000-0000-0000-000000000004",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := ioutil.TempDir("", "upload_backup")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			// create backupDir and add backup contents
			backupDir, err := ioutil.TempDir(tmpdir, "backupDir")
			require.Nil(t, err)
			createFiles(backupDir, tt.keys, true)

			// create backupDir and add backup contents
			destDir, err := ioutil.TempDir(tmpdir, "destDir")
			require.Nil(t, err)

			//test
			err = moveBackup(backupDir, destDir, tt.memberID)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			require.DirExists(t, path.Join(destDir, tt.want))
		})
	}
}
