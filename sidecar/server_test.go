package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
)

var exampleTarGzFiles = []fileutil.File{
	{Name: "cluster", IsDir: true},
	{Name: "cluster/cluster-state.txt", IsDir: false},
	{Name: "cluster/cluster-version.txt", IsDir: false},
	{Name: "cluster/partition-thread-count.bin", IsDir: false},
	{Name: "configs", IsDir: true},
	{Name: "s00", IsDir: true},
	{Name: "s00/tombstone", IsDir: true},
	{Name: "cluster/members.bin", IsDir: false},
	{Name: "s00/tombstone/02", IsDir: true},
	{Name: "s00/tombstone/02/0000000000000002.chunk", IsDir: false},
	{Name: "s00/value", IsDir: true},
	{Name: "s00/value/01", IsDir: true},
	{Name: "s00/value/01/0000000000000001.chunk", IsDir: false},
}

func TestBackupHandler(t *testing.T) {
	tmpDir := func(name string) string {
		file, err := os.MkdirTemp("", name)
		require.Nil(t, err)
		return file
	}
	tests := []struct {
		name           string
		body           Req
		files          []fileutil.File
		wantStatusCode int
		want           []string
	}{
		{
			"should work", Req{
				BackupBaseDir: tmpDir("working_path"),
				MemberID:      1,
			},
			[]fileutil.File{
				{Name: "backup-0000000000001", IsDir: true},
				{Name: "backup-0000000000001/00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "backup-0000000000001/00000000-0000-0000-0000-000000000002", IsDir: false},
				{Name: "backup-0000000000001/00000000-0000-0000-0000-000000000003", IsDir: true},
				{Name: "backup-0000000000001/wrong-id", IsDir: false},
				{Name: "backup-0000000000002", IsDir: false},
				{Name: "backup-0000000000004", IsDir: true},
				{Name: "backup-0000000000004/00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "backup-0000000000004/00000000-0000-0000-0000-000000000002", IsDir: true},
				{Name: "backup-0000000000003", IsDir: true},
				{Name: "backup-0000000000003/00000000-0000-0000-0000-000000000001", IsDir: true},
				{Name: "backup-0000000000003/00000000-0000-0000-0000-000000000002", IsDir: true},
			},
			http.StatusOK,
			[]string{"backup-0000000000001/00000000-0000-0000-0000-000000000003", "backup-0000000000003/00000000-0000-0000-0000-000000000002", "backup-0000000000004/00000000-0000-0000-0000-000000000002"},
		},
		{
			"should fail no backup dir exists", Req{
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
			bs := &Service{Tasks: map[uuid.UUID]*task{}}

			err := fileutil.CreateFiles(path.Join(tt.body.BackupBaseDir, DirName), tt.files, false)
			require.Nil(t, err)
			defer os.RemoveAll(tt.body.BackupBaseDir)

			bdy, err := json.Marshal(tt.body)
			bdyStr := string(bdy)
			require.Nil(t, err)
			req := httptest.NewRequest(http.MethodPost, "http://request/backup", strings.NewReader(bdyStr))
			w := httptest.NewRecorder()

			// Test
			bs.listBackupsHandler(w, req)
			res := w.Result()
			st := res.StatusCode
			require.Equal(t, tt.wantStatusCode, st, "Status is: ", st)
			if st != http.StatusOK {
				return
			}

			// Request was successful
			resBody := &Resp{}
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
			require.Nil(t, err)
			us := &Service{Tasks: map[uuid.UUID]*task{}}
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
			us.Tasks[resBody.ID].cancel()
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
			map[uuid.UUID]*task{stringToUUID(""): successfulTask(UploadReq{})},
			stringToUUID("").String(),
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
			stringToUUID("").String(),
			http.StatusNotFound,
			"",
		},
		{
			"task is in progress",
			map[uuid.UUID]*task{stringToUUID(""): inProgressTask(UploadReq{})},
			stringToUUID("").String(),
			http.StatusOK,
			"IN_PROGRESS",
		},
		{
			"task cancelled",
			map[uuid.UUID]*task{stringToUUID(""): cancelledTask(UploadReq{})},
			stringToUUID("").String(),
			http.StatusOK,
			"CANCELED",
		},
		{
			"task failed",
			map[uuid.UUID]*task{stringToUUID(""): failedTask(UploadReq{})},
			stringToUUID("").String(),
			http.StatusOK,
			"FAILURE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up

			us := &Service{Tasks: tt.taskMap}
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
			map[uuid.UUID]*task{stringToUUID(""): inProgressTask(UploadReq{})},
			stringToUUID("").String(),
			http.StatusOK,
		},
		{
			"should work for in successful task",
			map[uuid.UUID]*task{stringToUUID(""): successfulTask(UploadReq{})},
			stringToUUID("").String(),
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
			stringToUUID("").String(),
			http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			us := &Service{Tasks: tt.taskMap}
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://request/upload/%s/cancel", tt.reqId), nil)
			w := httptest.NewRecorder()
			vars := map[string]string{
				"id": tt.reqId,
			}
			req = mux.SetURLVars(req, vars)

			// Test
			us.cancelHandler(w, req)
			res := w.Result()
			st := res.StatusCode
			assert.Equal(t, tt.wantStatusCode, st, "Status is: ", st)
		})
	}
}

func TestDeleteHandler(t *testing.T) {
	tests := []struct {
		name  string
		tasks map[uuid.UUID]*task
		reqID string
		want  int
	}{
		{
			"in-progress task",
			map[uuid.UUID]*task{stringToUUID(""): inProgressTask(UploadReq{})},
			stringToUUID("").String(),
			http.StatusOK,
		},
		{
			"successful task",
			map[uuid.UUID]*task{stringToUUID(""): successfulTask(UploadReq{})},
			stringToUUID("").String(),
			http.StatusOK,
		},
		{
			"task is not in map",
			map[uuid.UUID]*task{},
			stringToUUID("").String(),
			http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			service := &Service{Tasks: tt.tasks}
			req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("http://request/upload/%s", tt.reqID), nil)
			req = mux.SetURLVars(req, map[string]string{
				"id": tt.reqID,
			})

			// Test
			w := httptest.NewRecorder()
			service.deleteHandler(w, req)
			statusCode := w.Result().StatusCode

			assert.Equal(t, tt.want, statusCode, "Status is: ", statusCode)

			w = httptest.NewRecorder()
			service.statusHandler(w, req)
			statusCode = w.Result().StatusCode

			assert.Equal(t, http.StatusNotFound, statusCode, "Status is: ", statusCode)
		})
	}
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
			tmpdir, err := os.MkdirTemp("", "upload_backup")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			// create backupDir and add backup contents
			backupDir, err := os.MkdirTemp(tmpdir, "backupDir")
			require.Nil(t, err)

			for _, id := range tt.keys {
				idPath := path.Join(backupDir, id)
				err = fileutil.CreateFiles(idPath, exampleTarGzFiles, true)
				require.Nil(t, err)
			}

			// copy the files under backupDir for checking later
			backupDirCopy := path.Join(tmpdir, "backupDirCopy")
			cmd := exec.Command("cp", "-R", backupDir, backupDirCopy)
			err = cmd.Run()
			require.Nil(t, err)

			// create bucket
			bucketPath, err := os.MkdirTemp(tmpdir, "bucket")
			require.Nil(t, err)
			bucket, err := fileblob.OpenBucket(bucketPath, nil)
			require.Nil(t, err)

			// Run test
			prefix := "prefix"
			backupKey, err := UploadBackup(ctx, bucket, backupDir, prefix, tt.memberID)
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
			err = CreateArchive(str, idPath, path.Base(idPath))
			require.Nil(t, err)

			// get the content of the tar in the bucket
			content, err := bucket.ReadAll(ctx, backupKey)
			require.Nil(t, err)

			require.Equal(t, str.String(), string(content))
		})
	}
}

func TestCreateArchive(t *testing.T) {
	_, err := exec.LookPath("tar")
	require.Nil(t, err, "Need tar executable for this test")

	tests := []struct {
		name    string
		want    []fileutil.File
		wantErr bool
	}{
		{
			"standard", exampleTarGzFiles, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up
			tmpdir, err := os.MkdirTemp("", "create_archive")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			tarGzipFilesDir := path.Join(tmpdir, "tarGzipFilesDir")
			err = fileutil.CreateFiles(tarGzipFilesDir, tt.want, true)
			require.Nil(t, err)

			tarGzipFile, err := os.OpenFile(path.Join(tmpdir, "file.tar.gz"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0700)
			require.Nil(t, err)

			// Test
			err = CreateArchive(tarGzipFile, tarGzipFilesDir, path.Base(tarGzipFilesDir))
			require.Nil(t, err)

			cmd := exec.Command("tar", "-tvf", tarGzipFile.Name())
			output, err := cmd.Output()
			require.Nil(t, err)

			var files []fileutil.File
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

				files = append(files, fileutil.File{IsDir: isDir, Name: fileName})
			}
			require.ElementsMatch(t, files, exampleTarGzFiles)
		})
	}

}

func stringToUUID(s string) uuid.UUID {
	var bytes16 [16]byte
	copy(bytes16[:], s)
	return bytes16
}

func cancelledTask(req UploadReq) *task {
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

func inProgressTask(req UploadReq) *task {
	ctx, cancel := context.WithCancel(context.Background())
	t := &task{
		req:    req,
		ctx:    ctx,
		cancel: cancel,
		err:    nil,
	}
	return t
}

func failedTask(req UploadReq) *task {
	ctx, cancel := context.WithCancel(context.Background())
	t := &task{
		req:    req,
		ctx:    ctx,
		cancel: cancel,
		err:    fmt.Errorf("task is failed"),
	}
	cancel()
	return t
}

func successfulTask(req UploadReq) *task {
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

func countSubstring(list []string, substr string) (count int) {
	for _, str := range list {
		if strings.Contains(str, substr) {
			count++
		}
	}
	return
}
