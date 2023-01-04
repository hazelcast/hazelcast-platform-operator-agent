package sidecar

import (
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/platform-operator-agent/internal/serverutil"
	"log"
	"net"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
)

const (
	DirName = "hot-backup"
)

// Service handles requests and keeps track of Tasks
type Service struct {
	Mu    sync.RWMutex
	Tasks map[uuid.UUID]*task
}

// Req is a backup Service backup method request
type Req struct {
	BackupBaseDir string `json:"backup_base_dir"`
	MemberID      int    `json:"member_id"`
}

// Resp is a backup Service backup method response
type Resp struct {
	Backups []string `json:"backups"`
}

func (s *Service) listBackupsHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	var req Req
	if err := serverutil.DecodeBody(r, &req); err != nil {
		log.Println("BACKUP", "Error occurred while parsing body:", err)
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	backupsDir := path.Join(req.BackupBaseDir, DirName)
	backupSeqs, err := fileutil.FolderSequence(backupsDir)
	if err != nil {
		log.Println("BACKUP", "Error reading backup sequence directory", err)
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	var backups []string
	for _, backupSeq := range backupSeqs {
		backupDir := path.Join(backupsDir, backupSeq.Name())
		backupUUIDs, err := fileutil.FolderUUIDs(backupDir)
		if err != nil {
			log.Println("BACKUP", "Error reading backup directory", err)
			serverutil.HttpError(w, http.StatusBadRequest)
			return
		}

		if len(backupUUIDs) != 1 && len(backupUUIDs) <= req.MemberID {
			log.Println("BACKUP", "Invalid UUID")
			serverutil.HttpError(w, http.StatusBadRequest)
			return
		}

		// If there is only one backup, members are isolated. No need for memberID
		if len(backupUUIDs) == 1 {
			log.Println("BACKUP", "Skip member ID")
			req.MemberID = 0
		}

		backupPath := path.Join(backupSeq.Name(), backupUUIDs[req.MemberID].Name())
		backups = append(backups, backupPath)

		log.Println("BACKUP", "Found backup", backupPath)
	}

	serverutil.HttpJSON(w, Resp{Backups: backups})
}

// UploadReq is a backup Service upload method request
type UploadReq struct {
	BucketURL       string `json:"bucket_url"`
	BackupBaseDir   string `json:"backup_base_dir"`
	HazelcastCRName string `json:"hz_cr_name"`
	SecretName      string `json:"secret_name"`
	MemberID        int    `json:"member_id"`
}

// UploadResp ia a backup Service upload method response
type UploadResp struct {
	ID uuid.UUID `json:"id"`
}

func (s *Service) uploadHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	var req UploadReq
	if err := serverutil.DecodeBody(r, &req); err != nil {
		log.Println("UPLOAD", "Error occurred while parsing body:", err)
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	ID, err := uuid.NewRandom()
	if err != nil {
		log.Println("UPLOAD", "Error occurred while generating new UUID:", err)
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &task{
		req:    req,
		ctx:    ctx,
		cancel: cancel,
	}

	s.Mu.Lock()
	s.Tasks[ID] = t
	s.Mu.Unlock()

	// run upload in background
	log.Println("UPLOAD", ID, "Starting new task")
	go t.process(ID)

	serverutil.HttpJSON(w, UploadResp{ID: ID})
}

// StatusResp is a backup Service task status response
type StatusResp struct {
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	BackupKey string `json:"backup_key,omitempty"`
}

func (s *Service) statusHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	vars := mux.Vars(r)

	ID, err := uuid.Parse(vars["id"])
	if err != nil {
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	s.Mu.RLock()
	t, ok := s.Tasks[ID]
	s.Mu.RUnlock()

	// unknown task
	if !ok {
		log.Println("STATUS", ID, "task not found")
		serverutil.HttpError(w, http.StatusNotFound)
		return
	}

	// context error is set to non-nil by the first cancel call
	if t.ctx.Err() == nil {
		log.Println("STATUS", ID, "task in progress")
		serverutil.HttpJSON(w, StatusResp{Status: "IN_PROGRESS"})
		return
	}

	// error from the task could be just info that it was canceled
	if errors.Is(t.err, context.Canceled) {
		log.Println("STATUS", ID, "task canceled")
		serverutil.HttpJSON(w, StatusResp{Status: "CANCELED", Message: t.err.Error()})
		return
	}

	// there was some actual error
	if t.err != nil {
		log.Println("STATUS", ID, "task failed")
		serverutil.HttpJSON(w, StatusResp{Status: "FAILURE", Message: t.err.Error()})
		return
	}

	log.Println("STATUS", ID, "task successful")
	serverutil.HttpJSON(w, StatusResp{Status: "SUCCESS", BackupKey: t.backupKey})
}

func (s *Service) cancelHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	vars := mux.Vars(r)

	ID, err := uuid.Parse(vars["id"])
	if err != nil {
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	s.Mu.RLock()
	t, ok := s.Tasks[ID]
	s.Mu.RUnlock()
	if !ok {
		log.Println("CANCEL", ID, "task not found")
		serverutil.HttpError(w, http.StatusNotFound)
		return
	}

	// send signal to stop task
	log.Println("CANCEL", ID, "Canceling task")
	t.cancel()
}

type DialRequest struct {
	Endpoints string `json:"endpoints"`
}

type DialResponse struct {
	Success bool `json:"success"`
}

func dialHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	var req DialRequest
	if err := serverutil.DecodeBody(r, &req); err != nil {
		log.Println("DIAL", "Error occurred while parsing body:", err)
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	var errs []error
	endpoints := strings.Split(req.Endpoints, ",")
	for _, e := range endpoints {
		err := tryDial(e)
		if err != nil {
			errStr := fmt.Errorf("%s is not reachable", e)
			errs = append(errs, errStr)
			log.Println(errStr)
		}
	}

	if len(errs) > 0 {
		serverutil.HttpJSON(w, DialResponse{Success: false})
	} else {
		serverutil.HttpJSON(w, DialResponse{Success: true})
	}
}

func tryDial(endpoint string) error {
	_, err := net.Dial("tcp", endpoint)
	if err != nil {
		return err
	}
	return nil
}

func healthcheckHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
