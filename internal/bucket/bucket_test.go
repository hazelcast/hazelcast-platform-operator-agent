package bucket

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

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
			err = saveFile(context.Background(), b, tt.key, dstPath)
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

func TestDownloadFiles(t *testing.T) {
	tests := []struct {
		name          string
		dstPathExists bool
		files         []fileutil.File
		wantFiles     []fileutil.File
		wantErr       bool
	}{
		{
			"all files allowed",
			true,
			[]fileutil.File{
				{Name: "file1"},
				{Name: "test1.jar"},
				{Name: "test2.class"},
			},
			[]fileutil.File{
				{Name: "file1"},
				{Name: "test1.jar"},
				{Name: "test2.class"}},
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
			"top level files",
			true,
			[]fileutil.File{
				{Name: "folder1/test2.jar"},
				{Name: "test1.jar2"},
				{Name: "jarjar"},
			},
			[]fileutil.File{
				{Name: "test1.jar2"},
				{Name: "jarjar"},
			},
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
			tmpdir, err := os.MkdirTemp("", "download_files")
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
			err = DownloadFiles(context.Background(), "file://"+bucketPath, dstPath, nil)
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

func TestSecretReader_SecretData(t *testing.T) {
	tests := []struct {
		name       string
		data       map[string][]byte
		secretName string
		errMsg     string
	}{
		{
			name:       "empty secret name",
			data:       nil,
			secretName: "",
			errMsg:     "",
		},
		{
			name: "secret with data",
			data: map[string][]byte{
				"region":            []byte("us-east-1"),
				"access-key-id":     []byte("<access-key-id>"),
				"secret-access-key": []byte("<secret-access-key>"),
			},
			secretName: "gke-bucket-secret",
			errMsg:     "",
		},
		{
			name:       "nonexisting secret name",
			data:       nil,
			secretName: "gke-bucket-secret",
			errMsg:     "secrets \"gke-bucket-secret\" not found",
		},
		{
			name:       "secret without data",
			data:       map[string][]byte{},
			secretName: "gke-bucket-secret",
			errMsg:     "data is empty in the bucket authentication secret: gke-bucket-secret",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := fake.NewSimpleClientset()
			sr := SecretReader{SecretInterface: c.CoreV1().Secrets("")}
			if test.secretName != "" && test.data != nil {
				sr = fakeSecretReader(test.secretName, test.data)
			}
			data, err := sr.SecretData(context.Background(), test.secretName)
			if test.errMsg != "" {
				require.EqualError(t, err, test.errMsg)
				require.Nil(t, data)
			} else {
				require.Nil(t, err)
				require.Equal(t, test.data, data)
			}
		})
	}
}

func fakeSecretReader(name string, data map[string][]byte) SecretReader {
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Data: data,
	}
	c := fake.NewSimpleClientset(&secret)
	return SecretReader{SecretInterface: c.CoreV1().Secrets("default")}
}
