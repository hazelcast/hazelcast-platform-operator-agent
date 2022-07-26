package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"path"
	"sync"

	"github.com/google/subcommands"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/hazelcast/platform-operator-agent/backup"
	"github.com/hazelcast/platform-operator-agent/bucket"
)

type backupCmd struct {
	address string
}

func (*backupCmd) Name() string     { return "backup" }
func (*backupCmd) Synopsis() string { return "run backup sidecar service" }
func (*backupCmd) Usage() string    { return "" }

func (p *backupCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&p.address, "address", ":8080", "http server listen address")
}

func (p *backupCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Println("Starting backup agent...")
	s := uploadService{
		tasks: make(map[uuid.UUID]*task),
	}
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/upload", s.uploadHandler).Methods("POST")
	router.HandleFunc("/upload/{id}", s.statusHandler).Methods("GET")
	router.HandleFunc("/upload/{id}", s.cancelHandler).Methods("DELETE")
	router.HandleFunc("/health", s.healthcheckHandler)
	if err := http.ListenAndServe(p.address, router); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

// uploadService handles requests and keeps track of tasks
type uploadService struct {
	mu    sync.RWMutex
	tasks map[uuid.UUID]*task
}

type uploadReq struct {
	BucketURL        string `json:"bucket_url"`
	BackupFolderPath string `json:"backup_folder_path"`
	HazelcastCRName  string `json:"hz_cr_name"`
	SecretName       string `json:"secret_name"`
}

type uploadResp struct {
	ID uuid.UUID `json:"id"`
}

func (s *uploadService) uploadHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	var req uploadReq
	if err := s.decodeBody(r, &req); err != nil {
		log.Println("Error occurred while parsing body:", err)
		s.httpError(w, http.StatusBadRequest)
		return
	}

	ID, err := uuid.NewRandom()
	if err != nil {
		log.Println("Error occurred while genereting new UUID:", err)
		s.httpError(w, http.StatusBadRequest)
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
	go t.process(ID)

	s.httpJSON(w, uploadResp{ID: ID})
}

type statusResp struct {
	Status string `json:"status"`
}

var (
	inProgressResp = statusResp{Status: "IN_PROGRESS"}
	canceledResp   = statusResp{Status: "CANCELED"}
	failureResp    = statusResp{Status: "FAILURE"}
	successResp    = statusResp{Status: "SUCCESS"}
)

func (s *uploadService) statusHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	vars := mux.Vars(r)

	ID, err := uuid.Parse(vars["id"])
	if err != nil {
		s.httpError(w, http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	t, ok := s.tasks[ID]
	s.mu.RUnlock()

	// unknown task
	if !ok {
		s.httpError(w, http.StatusNotFound)
		return
	}

	// context error is set to non-nil by the first cancel call
	if t.ctx.Err() == nil {
		s.httpJSON(w, inProgressResp)
		return
	}

	// error from the task could be just info that it was canceled
	if errors.Is(t.err, context.Canceled) {
		s.httpJSON(w, canceledResp)
		return
	}

	// there was some actual error
	if t.err != nil {
		s.httpJSON(w, failureResp)
		return
	}

	s.httpJSON(w, successResp)
}

func (s *uploadService) cancelHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	vars := mux.Vars(r)

	ID, err := uuid.Parse(vars["id"])
	if err != nil {
		s.httpError(w, http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	t, ok := s.tasks[ID]
	s.mu.RUnlock()
	if !ok {
		s.httpError(w, http.StatusNotFound)
		return
	}

	// send signal to stop task
	t.cancel()
}

func (s *uploadService) decodeBody(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	if err := d.Decode(v); err != nil {
		return err
	}
	log.Printf("BODY %+v", v)
	return nil
}

func (s *uploadService) httpError(w http.ResponseWriter, code int) {
	log.Println("ERROR", code)
	http.Error(w, http.StatusText(code), code)
}

func (s *uploadService) httpJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	if err := e.Encode(v); err != nil {
		s.httpError(w, http.StatusInternalServerError)
		return
	}
}

func (s *uploadService) healthcheckHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// task is an upload process that is cancelable
type task struct {
	req    uploadReq
	ctx    context.Context
	cancel context.CancelFunc
	err    error
}

func (t *task) process(ID uuid.UUID) {
	log.Println("TASK", ID, "started")
	defer log.Printf("TASK %s finished: %+v", ID, t)
	defer t.cancel()

	bucketURI, err := formatURI(t.req.BucketURL)
	if err != nil {
		log.Println("TASK", ID, "Error occurred while read parsing bucket URI:", err)
		t.err = err
		return
	}

	bucket, err := bucket.OpenBucket(t.ctx, bucketURI, t.req.SecretName)
	if err != nil {
		log.Println("TASK", ID, "openBucket:", err)
		t.err = err
		return
	}

	backupsDir := path.Join(t.req.BackupFolderPath, "hot-backup")
	err = backup.UploadBackup(t.ctx, bucket, backupsDir, t.req.HazelcastCRName)
	if err != nil {
		log.Println("TASK", ID, "uploadBackup:", err)
		t.err = err
		return
	}
}
