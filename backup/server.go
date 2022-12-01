package backup

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/hazelcast/platform-operator-agent/internal"
	"log"
	"net/http"
	"path"
	"sync"
)

// service handles requests and keeps track of tasks
type service struct {
	mu    sync.RWMutex
	tasks map[uuid.UUID]*task
}

// Req is a backup service backup method request
type Req struct {
	BackupBaseDir string `json:"backup_base_dir"`
	MemberID      int    `json:"member_id"`
}

// Resp is a backup service backup method response
type Resp struct {
	Backups []string `json:"backups"`
}

func (s *service) listBackupsHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	var req Req
	if err := decodeBody(r, &req); err != nil {
		log.Println("BACKUP", "Error occurred while parsing body:", err)
		httpError(w, http.StatusBadRequest)
		return
	}

	backupsDir := path.Join(req.BackupBaseDir, DirName)
	backupSeqs, err := internal.FolderSequence(backupsDir)
	if err != nil {
		log.Println("BACKUP", "Error reading backup sequence directory", err)
		httpError(w, http.StatusBadRequest)
		return
	}

	var backups []string
	for _, backupSeq := range backupSeqs {
		backupDir := path.Join(backupsDir, backupSeq.Name())
		backupUUIDs, err := internal.FolderUUIDs(backupDir)
		if err != nil {
			log.Println("BACKUP", "Error reading backup directory", err)
			httpError(w, http.StatusBadRequest)
			return
		}

		if len(backupUUIDs) != 1 && len(backupUUIDs) <= req.MemberID {
			log.Println("BACKUP", "Invalid UUID")
			httpError(w, http.StatusBadRequest)
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

	httpJSON(w, Resp{Backups: backups})
}

// UploadReq is a backup service upload method request
type UploadReq struct {
	BucketURL       string `json:"bucket_url"`
	BackupBaseDir   string `json:"backup_base_dir"`
	HazelcastCRName string `json:"hz_cr_name"`
	SecretName      string `json:"secret_name"`
	MemberID        int    `json:"member_id"`
}

// UploadResp ia a backup service upload method response
type UploadResp struct {
	ID uuid.UUID `json:"id"`
}

func (s *service) uploadHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	var req UploadReq
	if err := decodeBody(r, &req); err != nil {
		log.Println("UPLOAD", "Error occurred while parsing body:", err)
		httpError(w, http.StatusBadRequest)
		return
	}

	ID, err := uuid.NewRandom()
	if err != nil {
		log.Println("UPLOAD", "Error occurred while generating new UUID:", err)
		httpError(w, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &task{
		req:    req,
		ctx:    ctx,
		cancel: cancel,
	}

	s.mu.Lock()
	s.tasks[ID] = t
	s.mu.Unlock()

	// run upload in background
	log.Println("UPLOAD", ID, "Starting new task")
	go t.process(ID)

	httpJSON(w, UploadResp{ID: ID})
}

// StatusResp is a backup service task status response
type StatusResp struct {
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	BackupKey string `json:"backup_key,omitempty"`
}

func (s *service) statusHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	vars := mux.Vars(r)

	ID, err := uuid.Parse(vars["id"])
	if err != nil {
		httpError(w, http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	t, ok := s.tasks[ID]
	s.mu.RUnlock()

	// unknown task
	if !ok {
		log.Println("STATUS", ID, "Task not found")
		httpError(w, http.StatusNotFound)
		return
	}

	// context error is set to non-nil by the first cancel call
	if t.ctx.Err() == nil {
		log.Println("STATUS", ID, "Task in progress")
		httpJSON(w, StatusResp{Status: "IN_PROGRESS"})
		return
	}

	// error from the task could be just info that it was canceled
	if errors.Is(t.err, context.Canceled) {
		log.Println("STATUS", ID, "Task canceled")
		httpJSON(w, StatusResp{Status: "CANCELED", Message: t.err.Error()})
		return
	}

	// there was some actual error
	if t.err != nil {
		log.Println("STATUS", ID, "Task failed")
		httpJSON(w, StatusResp{Status: "FAILURE", Message: t.err.Error()})
		return
	}

	log.Println("STATUS", ID, "Task successful")
	httpJSON(w, StatusResp{Status: "SUCCESS", BackupKey: t.backupKey})
}

func (s *service) cancelHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	vars := mux.Vars(r)

	ID, err := uuid.Parse(vars["id"])
	if err != nil {
		httpError(w, http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	t, ok := s.tasks[ID]
	s.mu.RUnlock()
	if !ok {
		log.Println("CANCEL", ID, "Task not found")
		httpError(w, http.StatusNotFound)
		return
	}

	// send signal to stop task
	log.Println("CANCEL", ID, "Canceling task")
	t.cancel()
}

func (s *service) healthcheckHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func decodeBody(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	if err := d.Decode(v); err != nil {
		return err
	}
	log.Printf("BODY %+v", v)
	return nil
}

func httpError(w http.ResponseWriter, code int) {
	log.Println("ERROR", code)
	http.Error(w, http.StatusText(code), code)
}

func httpJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	if err := e.Encode(v); err != nil {
		httpError(w, http.StatusInternalServerError)
		return
	}
}
