package main

import (
	"backup-agent/backup"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

type uploadReq struct {
	BucketURL string `json:"bucket_url"`
	BackupFolderPath string `json:"backup_folder_path"`
	HazelcastCRName string `json:"hz_cr_name"`
}

func upload(w http.ResponseWriter, r *http.Request) {
	var req uploadReq
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, "Error occurred while read upload request's body.")
	}
	json.Unmarshal(reqBody, &req)
	err = backup.UploadBackup(context.Background(), req.BucketURL, req.BackupFolderPath, req.HazelcastCRName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/upload", upload).Methods("POST")
	log.Fatal(http.ListenAndServe(":8080", router))
}
