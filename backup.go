package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"sync"

	"github.com/google/subcommands"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/hazelcast/platform-operator-agent/backup"
	"github.com/hazelcast/platform-operator-agent/bucket"
	"golang.org/x/sync/errgroup"
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

// task is an upload process that is cancelable
type task struct {
	ctx    context.Context
	cancel context.CancelFunc
	err    error
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
	var req uploadReq
	if err := s.decodeBody(r, &req); err != nil {
		log.Println("Error occurred while parsing body:", err)
		s.httpError(w, http.StatusBadRequest)
		return
	}

	bucketURI, err := formatURI(req.BucketURL)
	if err != nil {
		log.Println("Error occurred while read parsing bucket URI:", err)
		s.httpError(w, http.StatusBadRequest)
		return
	}
	log.Printf("Request parameters are: %+v\n", req)

	ID, err := uuid.NewRandom()
	if err != nil {
		log.Println("Error occurred while genereting new UUID:", err)
		s.httpError(w, http.StatusBadRequest)
		return
	}

	g, ctx := errgroup.WithContext(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	t := &task{
		ctx:    ctx,
		cancel: cancel,
	}

	s.mu.Lock()
	s.tasks[ID] = t
	s.mu.Unlock()

	// run upload in background
	g.Go(func() error {
		defer log.Println(ID, "task finished")
		defer cancel()

		bucket, err := bucket.OpenBucket(ctx, bucketURI, req.SecretName)
		if err != nil {
			log.Println(ID, "openBucket:", err)
			t.err = err
			return err
		}

		err = backup.UploadBackup(ctx, bucket, req.BucketURL, req.BackupFolderPath, req.HazelcastCRName)
		if err != nil {
			log.Println(ID, "uploadBackup:", err)
			t.err = err
			return err
		}

		return nil
	})

	s.httpJSON(w, uploadResp{ID: ID})
}

type statusResp struct {
	Status string `json:"status"`
}

var (
	inprogressResp = statusResp{Status: "In progress"}
	canceledResp   = statusResp{Status: "Canceled"}
	errorResp      = statusResp{Status: "Error"}
	finishedResp   = statusResp{Status: "Finished"}
)

func (s *uploadService) statusHandler(w http.ResponseWriter, r *http.Request) {
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
		s.httpJSON(w, inprogressResp)
		return
	}

	// error from the task could be just info that it was canceled
	if errors.Is(t.err, context.Canceled) {
		s.httpJSON(w, canceledResp)
		return
	}

	// there was some actual error
	if t.err != nil {
		s.httpJSON(w, errorResp)
		return
	}

	s.httpJSON(w, finishedResp)
}

func (s *uploadService) cancelHandler(w http.ResponseWriter, r *http.Request) {
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
	return nil
}

func (s *uploadService) httpError(w http.ResponseWriter, code int) {
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
