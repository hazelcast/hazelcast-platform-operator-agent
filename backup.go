package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/subcommands"
	"github.com/gorilla/mux"
	"github.com/hazelcast/platform-operator-agent/backup"
	"io/ioutil"
	"log"
	"net/http"
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
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/upload", upload).Methods("POST")
	router.HandleFunc("/health", health)
	if err := http.ListenAndServe(p.address, router); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

type uploadReq struct {
	BucketURL        string `json:"bucket_url"`
	BackupFolderPath string `json:"backup_folder_path"`
	HazelcastCRName  string `json:"hz_cr_name"`
	SecretName       string `json:"secret_name"`
}

func upload(w http.ResponseWriter, r *http.Request) {
	var req uploadReq
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error occurred while read upload request's body.")
		return
	}
	json.Unmarshal(reqBody, &req)

	neededCredentials, err := backup.NeededCredentials(req.BucketURL)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Invalid cloud provider: %v", err)
		return
	}
	if err := backup.CreateCredentialsFromSecret(req.SecretName, neededCredentials); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error occurred while getting the secret for accessing cloud provider: %v", err)
		return
	}

	err = backup.UploadBackup(context.Background(), req.BucketURL, req.BackupFolderPath, req.HazelcastCRName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
