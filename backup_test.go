package main

import (
	"context"
	"encoding/json"
	"fmt"
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
		body           backupRequest
		files          []file
		wantStatusCode int
		want           []string
	}{
		{
			"should work", backupRequest{
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
			"should fail no backup dir exists", backupRequest{
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
			err := createFiles(path.Join(tt.body.BackupBaseDir, backupDirName), tt.files, false)
			require.Nil(t, err)
			defer os.RemoveAll(tt.body.BackupBaseDir)

			bdy, err := json.Marshal(tt.body)
			bdyStr := string(bdy)
			require.Nil(t, err)
			req := httptest.NewRequest(http.MethodPost, "http://request/backup", strings.NewReader(bdyStr))
			w := httptest.NewRecorder()
			bs := backupService{}

			// Test
			bs.backupHandler(w, req)
			res := w.Result()
			st := res.StatusCode
			require.Equal(t, tt.wantStatusCode, st, "Status is: ", st)
			if st != http.StatusOK {
				return
			}

			// Request was successful
			resBody := &backupResponse{}
			defer res.Body.Close()
			d := json.NewDecoder(res.Body)
			err = d.Decode(resBody)
			require.Nil(t, err)
			require.Equal(t, tt.want, resBody.Backups)

		})
	}
}

func TestUploadHandler(t *testing.T) {
	uq := &uploadReq{
		BucketURL:        "",
		BackupFolderPath: "",
		HazelcastCRName:  "",
		SecretName:       "",
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
			us := &uploadService{tasks: map[uuid.UUID]*task{}}
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
			resBody := &uploadResp{}
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
		wantResponse   *statusResp
	}{
		{
			"should work",
			map[uuid.UUID]*task{getUUIDFrom(""): getSuccessfulTask(uploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
			nil,
		},
		{
			"uuid parse error",
			map[uuid.UUID]*task{},
			"incorrect-uuid",
			http.StatusBadRequest,
			nil,
		},
		{
			"task is not in map",
			map[uuid.UUID]*task{},
			getUUIDFrom("").String(),
			http.StatusNotFound,
			nil,
		},
		{
			"task is in progress",
			map[uuid.UUID]*task{getUUIDFrom(""): getInProgressTask(uploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
			&inProgressResp,
		},
		{
			"task cancelled",
			map[uuid.UUID]*task{getUUIDFrom(""): getCancelledTask(uploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
			&canceledResp,
		},
		{
			"task failed",
			map[uuid.UUID]*task{getUUIDFrom(""): getFailedTask(uploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
			&failureResp,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			us := &uploadService{tasks: tt.taskMap}
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

			if tt.wantResponse == nil {
				return
			}
			status := &statusResp{}
			defer res.Body.Close()
			d := json.NewDecoder(res.Body)
			err := d.Decode(status)
			require.Nil(t, err)
			require.Equal(t, *tt.wantResponse, *status)

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
			map[uuid.UUID]*task{getUUIDFrom(""): getInProgressTask(uploadReq{})},
			getUUIDFrom("").String(),
			http.StatusOK,
		},
		{
			"should work for in successful task",
			map[uuid.UUID]*task{getUUIDFrom(""): getSuccessfulTask(uploadReq{})},
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
			us := &uploadService{tasks: tt.taskMap}
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

func getCancelledTask(req uploadReq) *task {
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

func getInProgressTask(req uploadReq) *task {
	ctx, cancel := context.WithCancel(context.Background())
	t := &task{
		req:    req,
		ctx:    ctx,
		cancel: cancel,
		err:    nil,
	}
	return t
}

func getFailedTask(req uploadReq) *task {
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
func getSuccessfulTask(req uploadReq) *task {
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
		name    string
		keys    []string
		want    []string
		wantErr bool
	}{
		{
			"empty backup dir",
			[]string{},
			nil,
			true,
		},
		{
			"sequence is not in correct form",
			[]string{
				"backupp-1659034855438/id",
			},
			nil,
			true,
		},
		{
			"sequence is not in correct form2",
			[]string{
				"backup-16abc855438/id",
			},
			nil,
			true,
		},
		{
			"single backup sequence single backup",
			[]string{
				"backup-1659034855438/id1",
			},
			[]string{"2022-07-28-19-00-55/id1.tar.gz"},
			false,
		},
		{
			"single backup sequence multiple backups",
			[]string{
				"backup-1659034855438/id1",
				"backup-1659035130065/id2",
			},
			[]string{
				"2022-07-28-19-00-55/id1.tar.gz",
				"2022-07-28-19-05-30/id2.tar.gz",
			},
			false,
		},
		{
			"multiple backup sequence multiple backups",
			[]string{
				"backup-1659034855438/id1",
				"backup-1659035130065/id2",
				"backup-1659035448800/id3",
				"backup-1659035448800/id4",
			},
			[]string{
				"2022-07-28-19-00-55/id1.tar.gz",
				"2022-07-28-19-05-30/id2.tar.gz",
				"2022-07-28-19-10-48/id3.tar.gz",
				"2022-07-28-19-10-48/id4.tar.gz",
			},
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

			prefix := "prefix"

			// Run test
			err = backup.UploadBackup(ctx, bucket, backupDir, prefix)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}

			for i, want := range tt.want {
				// check if tar exists in the bucket
				key := path.Join(prefix, want)
				exists, err := bucket.Exists(ctx, key)
				assert.Nil(t, err)
				assert.True(t, exists, "Following item is not in the bucket ", want)

				// create tar.gz for the backup folder tt.keys[i]
				str := new(strings.Builder)
				idPath := path.Join(backupDirCopy, tt.keys[i])
				err = backup.CreateArchieve(str, idPath, path.Base(idPath))
				assert.Nil(t, err)

				// get the content of the tar in the bucket
				content, err := bucket.ReadAll(ctx, key)
				assert.Nil(t, err)

				assert.Equal(t, str.String(), string(content))
			}
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
