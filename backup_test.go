package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/hazelcast/platform-operator-agent/backup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
)

func TestBackupHandler(t *testing.T) {
	tmpDir := func(name string) string {
		file, err := ioutil.TempDir("", name)
		require.Nil(t, err)
		return file
	}
	tests := []struct {
		name           string
		body           BackupReq
		files          []file
		wantStatusCode int
		want           []string
	}{
		{
			"should work", BackupReq{
				BackupBaseDir: tmpDir("working_path"),
				MemberID:      1,
			},
			[]file{
				{name: "backup-0000000000001", isDir: true},
				{name: "backup-0000000000001/00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "backup-0000000000001/00000000-0000-0000-0000-000000000002", isDir: false},
				{name: "backup-0000000000001/00000000-0000-0000-0000-000000000003", isDir: true},
				{name: "backup-0000000000001/wrong-id", isDir: false},
				{name: "backup-0000000000002", isDir: false},
				{name: "backup-0000000000004", isDir: true},
				{name: "backup-0000000000004/00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "backup-0000000000004/00000000-0000-0000-0000-000000000002", isDir: true},
				{name: "backup-0000000000003", isDir: true},
				{name: "backup-0000000000003/00000000-0000-0000-0000-000000000001", isDir: true},
				{name: "backup-0000000000003/00000000-0000-0000-0000-000000000002", isDir: true},
			},
			http.StatusOK,
			[]string{"backup-0000000000001/00000000-0000-0000-0000-000000000003", "backup-0000000000003/00000000-0000-0000-0000-000000000002", "backup-0000000000004/00000000-0000-0000-0000-000000000002"},
		},
		{
			"should fail no backup dir exists", BackupReq{
				BackupBaseDir: "does-not-exist",
			},
			nil,
			http.StatusBadRequest,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			bs := &backupService{tasks: map[uuid.UUID]*task{}}

			err := createFiles(path.Join(tt.body.BackupBaseDir, backupDirName), tt.files, false)
			require.Nil(t, err)
			defer os.RemoveAll(tt.body.BackupBaseDir)

			bdy, err := json.Marshal(tt.body)
			bdyStr := string(bdy)
			require.Nil(t, err)
			req := httptest.NewRequest(http.MethodPost, "http://request/backup", strings.NewReader(bdyStr))
			w := httptest.NewRecorder()

			// Test
			bs.backupHandler(w, req)
			res := w.Result()
			st := res.StatusCode
			require.Equal(t, tt.wantStatusCode, st, "Status is: ", st)
			if st != http.StatusOK {
				return
			}

			// Request was successful
			resBody := &BackupResp{}
			defer res.Body.Close()
			d := json.NewDecoder(res.Body)
			err = d.Decode(resBody)
			require.Nil(t, err)
			require.Equal(t, tt.want, resBody.Backups)

		})
	}
}

func TestUploadHandler(t *testing.T) {
	uq := &UploadReq{
		BucketURL:       "",
		BackupBaseDir:   "",
		HazelcastCRName: "",
		SecretName:      "",
	}
	uqb, err := json.Marshal(uq)
	uqStr := string(uqb)
	require.Nil(t, err)

	tests := []struct {
		name           string
		body           string
		wantStatusCode int
	}{
		{
			"should work", uqStr, http.StatusOK,
		},
		{
			"incorrect body", "false-body", http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			us := &backupService{tasks: map[uuid.UUID]*task{}}
			req := httptest.NewRequest(http.MethodPost, "http://request/upload", strings.NewReader(tt.body))
			w := httptest.NewRecorder()

			// Test
			us.uploadHandler(w, req)
			res := w.Result()
			st := res.StatusCode
			require.Equal(t, tt.wantStatusCode, st, "Status is: ", st)
			if st != http.StatusOK {
				return
			}

			// Request was successful
			resBody := &UploadResp{}
			defer res.Body.Close()
			d := json.NewDecoder(res.Body)
			err = d.Decode(resBody)
			require.Nil(t, err)
			require.NotEmpty(t, resBody.ID)

			//clean up
			us.tasks[resBody.ID].cancel()
		})
	}
}

func TestStatusHandler(t *testing.T) {
	tests := []struct {
		name           string
		taskMap        map[uuid.UUID]*task
		reqId          string
		wantStatusCode int
		wantStatus     string
	}{
		{
			"should work",
			map[uuid.UUID]*task{getUUIDFrom(""): getSuccessfulTask(UploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
			"",
		},
		{
			"uuid parse error",
			map[uuid.UUID]*task{},
			"incorrect-uuid",
			http.StatusBadRequest,
			"",
		},
		{
			"task is not in map",
			map[uuid.UUID]*task{},
			getUUIDFrom("").String(),
			http.StatusNotFound,
			"",
		},
		{
			"task is in progress",
			map[uuid.UUID]*task{getUUIDFrom(""): getInProgressTask(UploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
			"IN_PROGRESS",
		},
		{
			"task cancelled",
			map[uuid.UUID]*task{getUUIDFrom(""): getCancelledTask(UploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
			"CANCELED",
		},
		{
			"task failed",
			map[uuid.UUID]*task{getUUIDFrom(""): getFailedTask(UploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
			"FAILURE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			us := &backupService{tasks: tt.taskMap}
			req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://request/upload/%s", tt.reqId), nil)
			w := httptest.NewRecorder()
			vars := map[string]string{
				"id": tt.reqId,
			}
			req = mux.SetURLVars(req, vars)

			// Test
			us.statusHandler(w, req)
			res := w.Result()
			st := res.StatusCode
			assert.Equal(t, tt.wantStatusCode, st, "Status is: ", st)

			if tt.wantStatus == "" {
				return
			}
			status := &StatusResp{}
			defer res.Body.Close()
			d := json.NewDecoder(res.Body)
			err := d.Decode(status)
			require.Nil(t, err)
			require.Equal(t, tt.wantStatus, status.Status)

		})
	}
}

func TestCancelHandler(t *testing.T) {
	tests := []struct {
		name           string
		taskMap        map[uuid.UUID]*task
		reqId          string
		wantStatusCode int
	}{
		{
			"should work for in progress task",
			map[uuid.UUID]*task{getUUIDFrom(""): getInProgressTask(UploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
		},
		{
			"should work for in successful task",
			map[uuid.UUID]*task{getUUIDFrom(""): getSuccessfulTask(UploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
		},
		{
			"uuid parse error",
			map[uuid.UUID]*task{},
			"incorrect-uuid",
			http.StatusBadRequest,
		},
		{
			"task is not in map",
			map[uuid.UUID]*task{},
			getUUIDFrom("").String(),
			http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			us := &backupService{tasks: tt.taskMap}
			req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("http://request/upload/%s", tt.reqId), nil)
			w := httptest.NewRecorder()
			vars := map[string]string{
				"id": tt.reqId,
			}
			req = mux.SetURLVars(req, vars)

			// Test
			us.statusHandler(w, req)
			res := w.Result()
			st := res.StatusCode
			assert.Equal(t, tt.wantStatusCode, st, "Status is: ", st)
		})
	}
}

func getUUIDFrom(s string) uuid.UUID {
	var bytes16 [16]byte
	copy(bytes16[:], s)
	return uuid.UUID(bytes16)
}

func getCancelledTask(req UploadReq) *task {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t := &task{
		req:    req,
		ctx:    ctx,
		cancel: cancel,
		err:    context.Canceled,
	}
	return t
}

func getInProgressTask(req UploadReq) *task {
	ctx, cancel := context.WithCancel(context.Background())
	t := &task{
		req:    req,
		ctx:    ctx,
		cancel: cancel,
		err:    nil,
	}
	return t
}

func getFailedTask(req UploadReq) *task {
	ctx, cancel := context.WithCancel(context.Background())
	t := &task{
		req:    req,
		ctx:    ctx,
		cancel: cancel,
		err:    fmt.Errorf("Task is failed"),
	}
	cancel()
	return t
}
func getSuccessfulTask(req UploadReq) *task {
	ctx, cancel := context.WithCancel(context.Background())
	t := &task{
		req:    req,
		ctx:    ctx,
		cancel: cancel,
		err:    nil,
	}
	cancel()
	return t
}

func TestUploadBackup(t *testing.T) {
	tests := []struct {
		name       string
		memberID   int
		keys       []string
		want       string
		wantBucket string
		wantErr    bool
	}{
		{
			"empty backup dir",
			0,
			[]string{},
			"",
			"",
			true,
		},
		{
			"sequence is not in correct form",
			0,
			[]string{
				"backupp-1659034855438/00000000-0000-0000-0000-000000000001",
			},
			"",
			"",
			true,
		},
		{
			"uuid is not in correct form ",
			0,
			[]string{
				"backup-1659034855438/00000000-0000-0000-0000-1",
			},
			"",
			"",
			true,
		},
		{
			"sequence is not in correct form2",
			0,
			[]string{
				"backup-16abc855438/00000000-0000-0000-0000-000000000001",
			},
			"",
			"",
			true,
		},
		{
			"single backup sequence single backup",
			0,
			[]string{
				"backup-1659034855438/00000000-0000-0000-0000-000000000001",
			},
			"backup-1659034855438/00000000-0000-0000-0000-000000000001",
			"2022-07-28-19-00-55/00000000-0000-0000-0000-000000000001.tar.gz",
			false,
		},
		{
			"member id is incorrect but isolated members",
			4,
			[]string{
				"backup-1659035130065/00000000-0000-0000-0000-000000000002",
			},
			"backup-1659035130065/00000000-0000-0000-0000-000000000002",
			"2022-07-28-19-05-30/00000000-0000-0000-0000-000000000002.tar.gz",
			false,
		},
		{
			"member ID is out of index error",
			2,
			[]string{
				"backup-1659035130065/00000000-0000-0000-0000-000000000001",
				"backup-1659035130065/00000000-0000-0000-0000-000000000002",
			},
			"",
			"",
			true,
		},
		{
			"single backup sequence multiple backups",
			1,
			[]string{
				"backup-1659035130065/00000000-0000-0000-0000-000000000001",
				"backup-1659035130065/00000000-0000-0000-0000-000000000002",
			},
			"backup-1659035130065/00000000-0000-0000-0000-000000000002",
			"2022-07-28-19-05-30/00000000-0000-0000-0000-000000000002.tar.gz",
			false,
		},
		{
			"multiple backup sequence multiple backups",
			0,
			[]string{
				"backup-1659034855438/00000000-0000-0000-0000-000000000001",
				"backup-1659035130065/00000000-0000-0000-0000-000000000002",
				"backup-1659035448800/00000000-0000-0000-0000-000000000003",
				"backup-1659035448800/00000000-0000-0000-0000-000000000004",
			},
			"backup-1659035448800/00000000-0000-0000-0000-000000000003",
			"2022-07-28-19-10-48/00000000-0000-0000-0000-000000000003.tar.gz",
			false,
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := ioutil.TempDir("", "upload_backup")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			// create backupDir and add backup contents
			backupDir, err := ioutil.TempDir(tmpdir, "backupDir")
			require.Nil(t, err)

			for _, id := range tt.keys {
				idPath := path.Join(backupDir, id)
				err = createFiles(idPath, exampleTarGzFiles, true)
				require.Nil(t, err)
			}

			// copy the files under backupDir for checking later
			backupDirCopy := path.Join(tmpdir, "backupDirCopy")
			cmd := exec.Command("cp", "--recursive", backupDir, backupDirCopy)
			err = cmd.Run()
			require.Nil(t, err)

			// create bucket
			bucketPath, err := ioutil.TempDir(tmpdir, "bucket")
			require.Nil(t, err)
			bucket, err := fileblob.OpenBucket(bucketPath, nil)
			require.Nil(t, err)

			// Run test
			prefix := "prefix"
			backupKey, err := backup.UploadBackup(ctx, bucket, backupDir, prefix, tt.memberID)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			require.Equal(t, path.Join(prefix, tt.wantBucket), backupKey)

			// check if backup sequence is deleted or member backup is marked to be deleted
			if countSubstring(tt.keys, path.Dir(tt.want)) <= 1 {
				require.NoDirExists(t, path.Join(backupDir, path.Dir(tt.want)))
			} else {
				require.FileExists(t, path.Join(backupDir, tt.want+".delete"))
			}

			// check if only one tar exists in the bucket
			it := bucket.List(nil)
			obj, err := it.Next(ctx)
			require.Nil(t, err)
			require.Contains(t, obj.Key, path.Base(tt.want))
			_, err = it.Next(ctx)
			require.True(t, err == io.EOF, "Error is", err)

			// create tar.gz for the backup folder tt.keys[i]
			str := new(strings.Builder)
			idPath := path.Join(backupDirCopy, tt.want)
			err = backup.CreateArchieve(str, idPath, path.Base(idPath))
			require.Nil(t, err)

			// get the content of the tar in the bucket
			content, err := bucket.ReadAll(ctx, backupKey)
			require.Nil(t, err)

			require.Equal(t, str.String(), string(content))
		})
	}
}

func TestCreateArchieve(t *testing.T) {
	_, err := exec.LookPath("tar")
	require.Nil(t, err, "Need tar executable for this test")

	tests := []struct {
		name    string
		want    []file
		wantErr bool
	}{
		{
			"standard", exampleTarGzFiles, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := ioutil.TempDir("", "create_archieve")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			tarGzipFilesDir := path.Join(tmpdir, "tarGzipFilesDir")
			err = createFiles(tarGzipFilesDir, tt.want, true)
			require.Nil(t, err)

			tarGzipFile, err := os.OpenFile(path.Join(tmpdir, "file.tar.gz"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0700)
			require.Nil(t, err)

			// Test
			err = backup.CreateArchieve(tarGzipFile, tarGzipFilesDir, path.Base(tarGzipFilesDir))
			require.Nil(t, err)

			cmd := exec.Command("tar", "-tvf", tarGzipFile.Name())
			output, err := cmd.Output()
			require.Nil(t, err)

			var files []file
			// lines are in form "drwx------ user/group           0 2022-07-29 00:10 tarGzipFilesDir"
			for _, line := range strings.Split(strings.TrimSuffix(string(output), "\n"), "\n") {
				// check if file is a dir
				isDir := false
				if strings.HasPrefix(line, "d") {
					isDir = true
				}
				// remove folder name prefix from file name
				slice := strings.Split(line, " ")
				filePath := slice[len(slice)-1]
				if filePath == path.Base(tarGzipFilesDir) {
					continue
				}
				fileName := strings.TrimPrefix(filePath, path.Base(tarGzipFilesDir)+"/")

				files = append(files, file{isDir: isDir, name: fileName})
			}
			require.ElementsMatch(t, files, exampleTarGzFiles)
		})
	}

}

func countSubstring(list []string, substr string) (count int) {
	for _, str := range list {
		if strings.Contains(str, substr) {
			count++
		}
	}
	return
}
