package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

type file struct {
	name  string
	isDir bool
}

func TestDownloadClassJars(t *testing.T) {
	tests := []struct {
		name          string
		dstPathExists bool
		files         []file
		wantFiles     []file
		wantErr       bool
	}{
		{"only jar allowed", true,
			[]file{{name: "file1"}, {name: "test1.jar"}, {name: "test2.class"}},
			[]file{{name: "test1.jar"}}, false},
		{"no subfolder jars allowed", true,
			[]file{{name: "folder1/test2.jar"}, {name: "test1.jar"}, {name: "test2.jar"}},
			[]file{{name: "test1.jar"}, {name: "test2.jar"}}, false},
		{"no jar", true,
			[]file{{name: "folder1/test2.jar"}, {name: "test1.jar2"}, {name: "jarjar"}},
			[]file{}, false},
		{"dest path does not exist", false,
			[]file{{name: "test1.jar"}},
			[]file{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare the buckets and destination folder
			tmpdir, err := ioutil.TempDir("", "download_class_jars")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			bucketPath := path.Join(tmpdir, "bucket")
			err = createFiles(bucketPath, tt.files)
			require.Nil(t, err)

			var dstPath string
			if tt.dstPathExists {
				dstPath, err = ioutil.TempDir(tmpdir, "dest")
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
			copiedFiles, err := getDirFileList(dstPath)
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
			tmpdir, err := ioutil.TempDir("", "save_file_from_backup")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			bucket := memblob.OpenBucket(nil)
			defer bucket.Close()
			err = bucket.WriteAll(context.Background(), tt.key, []byte("content"), nil)
			require.Nil(t, err)

			var dstPath string
			if tt.dstPathExists {
				dstPath, err = ioutil.TempDir(tmpdir, "dest")
				require.Nil(t, err, "Destination Path could not be created")
			} else {
				dstPath = path.Join(tmpdir, "dest-does-not-exist")
			}

			// Run the tests
			err = saveFileFromBucket(context.Background(), bucket, tt.key, dstPath)
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

func createFiles(pth string, files []file) error {
	// create pth dir regardless
	err := os.MkdirAll(pth, 0700)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.isDir {
			err := os.MkdirAll(path.Join(pth, file.name), 0700)
			if err != nil {
				return err
			}
			continue
		}

		err := os.MkdirAll(path.Join(pth, path.Dir(file.name)), 0700)
		if err != nil {
			return err
		}

		_, err = os.Create(path.Join(pth, file.name))
		if err != nil {
			return err
		}
	}
	return nil
}

func getDirFileList(baseDir string) ([]file, error) {
	files := []file{}
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if baseDir == path {
			return nil
		}
		fileName := strings.TrimPrefix(path, baseDir+"/")
		files = append(files, file{name: fileName, isDir: info.IsDir()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
