package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"sync"

	"github.com/google/subcommands"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/sync/errgroup"

	"github.com/hazelcast/platform-operator-agent/backup"
	"github.com/hazelcast/platform-operator-agent/bucket"
)

type backupCmd struct {
	HTTPAddress  string `envconfig:"BACKUP_HTTP_ADDRESS"`
	HTTPSAddress string `envconfig:"BACKUP_HTTPS_ADDRESS"`
	CA           string `envconfig:"BACKUP_CA"`
	Cert         string `envconfig:"BACKUP_CERT"`
	Key          string `envconfig:"BACKUP_KEY"`
}

func (*backupCmd) Name() string     { return "backup" }
func (*backupCmd) Synopsis() string { return "run backup sidecar service" }
func (*backupCmd) Usage() string    { return "" }

func (p *backupCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&p.HTTPAddress, "http-address", ":8080", "http server listen address")
	f.StringVar(&p.HTTPSAddress, "https-address", ":8443", "https server listen address")
	f.StringVar(&p.CA, "ca", "ca.crt", "http server client ca")
	f.StringVar(&p.Cert, "cert", "tls.crt", "http server tls cert")
	f.StringVar(&p.Key, "key", "tls.key", "http server tls key")
}

func (p *backupCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Println("Starting backup agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("backup", p); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	ca, err := ioutil.ReadFile(p.CA)
	if err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(ca); !ok {
		log.Println("failed to find any PEM data in ca input")
		return subcommands.ExitFailure
	}

	s := uploadService{
		tasks: make(map[uuid.UUID]*task),
	}

	var g errgroup.Group
	g.Go(func() error {
		router := mux.NewRouter().StrictSlash(true)
		router.HandleFunc("/upload", s.uploadHandler).Methods("POST")
		router.HandleFunc("/upload/{id}", s.statusHandler).Methods("GET")
		router.HandleFunc("/upload/{id}", s.cancelHandler).Methods("DELETE")
		router.HandleFunc("/health", s.healthcheckHandler)
		server := &http.Server{
			Addr:    p.HTTPSAddress,
			Handler: router,
			TLSConfig: &tls.Config{
				ClientAuth: tls.RequireAndVerifyClientCert,
				ClientCAs:  pool,
			},
		}
		return server.ListenAndServeTLS(p.Cert, p.Key)
	})

	g.Go(func() error {
		router := http.NewServeMux()
		router.HandleFunc("/health", s.healthcheckHandler)
		return http.ListenAndServe(p.HTTPAddress, router)
	})

	if err := g.Wait(); err != nil {
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

	secretData, err := bucket.GetSecretData(t.ctx, t.req.SecretName)
	if err != nil {
		log.Println("TASK", ID, "Error occured while fetching secret", err)
		t.err = err
		return
	}

	bucket, err := bucket.OpenBucket(t.ctx, bucketURI, secretData)
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
