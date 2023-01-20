package sidecar

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"net"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
	"github.com/hazelcast/platform-operator-agent/internal/logger"
	"github.com/hazelcast/platform-operator-agent/internal/serverutil"
)

const (
	DirName = "hot-backup"
)

var routerLog = logger.New().Named("router")

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
	var req Req
	if err := serverutil.DecodeBody(r, &req); err != nil {
		routerLog.Error("error occurred while parsing body: " + err.Error())
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	backupsDir := path.Join(req.BackupBaseDir, DirName)
	backupSeqs, err := fileutil.FolderSequence(backupsDir)
	if err != nil {
		routerLog.Error("error reading backup sequence directory: " + err.Error())
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	var backups []string
	for _, backupSeq := range backupSeqs {
		backupDir := path.Join(backupsDir, backupSeq.Name())
		backupUUIDs, err := fileutil.FolderUUIDs(backupDir)
		if err != nil {
			routerLog.Error("error reading backup directory: " + err.Error())
			serverutil.HttpError(w, http.StatusBadRequest)
			return
		}

		if len(backupUUIDs) != 1 && len(backupUUIDs) <= req.MemberID {
			err = fmt.Errorf("invalid UUID")
			routerLog.Error(err.Error())
			serverutil.HttpError(w, http.StatusBadRequest)
			return
		}

		// If there is only one backup, members are isolated. No need for memberID
		if len(backupUUIDs) == 1 {
			routerLog.Info("skip member ID")
			req.MemberID = 0
		}

		backupPath := path.Join(backupSeq.Name(), backupUUIDs[req.MemberID].Name())
		backups = append(backups, backupPath)

		routerLog.Info("found backup", zap.String("backup path", backupPath))
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
	var req UploadReq
	if err := serverutil.DecodeBody(r, &req); err != nil {
		routerLog.Error("error occurred while parsing body: " + err.Error())
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	ID, err := uuid.NewRandom()
	if err != nil {
		routerLog.Error("error occurred while generating new UUID: " + err.Error())
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
	routerLog.Info("Starting new task", zap.Uint32("task id", ID.ID()))
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
		routerLog.Error("task not found", zap.Uint32("task id", ID.ID()))
		serverutil.HttpError(w, http.StatusNotFound)
		return
	}

	// context error is set to non-nil by the first cancel call
	if t.ctx.Err() == nil {
		routerLog.Info("task is in progress: ", zap.Uint32("task id", ID.ID()))
		serverutil.HttpJSON(w, StatusResp{Status: "IN_PROGRESS"})
		return
	}

	// error from the task could be just info that it was canceled
	if errors.Is(t.err, context.Canceled) {
		routerLog.Info("task is canceled: ", zap.Uint32("task id", ID.ID()))
		serverutil.HttpJSON(w, StatusResp{Status: "CANCELED", Message: t.err.Error()})
		return
	}

	// there was some actual error
	if t.err != nil {
		routerLog.Info("task is failed", zap.Uint32("task id", ID.ID()))
		serverutil.HttpJSON(w, StatusResp{Status: "FAILURE", Message: t.err.Error()})
		return
	}

	routerLog.Info("task is successful", zap.Uint32("task id", ID.ID()))
	serverutil.HttpJSON(w, StatusResp{Status: "SUCCESS", BackupKey: t.backupKey})
}

func (s *Service) cancelHandler(w http.ResponseWriter, r *http.Request) {
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
		routerLog.Error("task not found", zap.Uint32("task id", ID.ID()))
		serverutil.HttpError(w, http.StatusNotFound)
		return
	}

	// send signal to stop task
	routerLog.Info("canceling task", zap.Uint32("task id", ID.ID()))
	t.cancel()
}

type DialRequest struct {
	Endpoints string `json:"endpoints"`
}

type DialResponse struct {
	Success       bool     `json:"success"`
	ErrorMessages []string `json:"error_messages"`
}

type DialService struct{}

func (d *DialService) dialHandler(w http.ResponseWriter, r *http.Request) {
	var req DialRequest
	if err := serverutil.DecodeBody(r, &req); err != nil {
		routerLog.Error("error occurred while parsing body: " + err.Error())
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	dialResp := DialResponse{Success: true}
	endpoints := strings.Split(req.Endpoints, ",")

	var wg sync.WaitGroup
	for _, e := range endpoints {
		wg.Add(1)
		e := e
		go func() {
			defer wg.Done()
			err := tryDial(e)
			if err != nil {
				dialResp.Success = false
				dialResp.ErrorMessages = append(dialResp.ErrorMessages, fmt.Sprintf("%s is not reachable", e))
				routerLog.Error("target is not reachable", zap.String("target", e))
			}
		}()
	}
	wg.Wait()

	if len(dialResp.ErrorMessages) > 0 {
		serverutil.HttpJSON(w, dialResp)
	} else {
		serverutil.HttpJSON(w, dialResp)
	}
}

func tryDial(endpoint string) error {
	_, err := net.DialTimeout("tcp", endpoint, 3*time.Second)
	if err != nil {
		return err
	}
	return nil
}

func healthcheckHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
