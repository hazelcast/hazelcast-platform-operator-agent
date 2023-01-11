package sidecar

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"
)

func startServer(s *Cmd) error {
	ca, err := os.ReadFile(s.CA)
	if err != nil {
		log.Println(err)
		return err
	}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(ca); !ok {
		log.Println("failed to find any PEM data in ca input")
		return err
	}

	backupService := Service{
		Tasks: make(map[uuid.UUID]*task),
	}

	var g errgroup.Group
	g.Go(func() error {
		router := mux.NewRouter().StrictSlash(true)
		router.HandleFunc("/backup", backupService.listBackupsHandler).Methods("GET")
		router.HandleFunc("/upload", backupService.uploadHandler).Methods("POST")
		router.HandleFunc("/upload/{id}", backupService.statusHandler).Methods("GET")
		router.HandleFunc("/upload/{id}", backupService.cancelHandler).Methods("DELETE")
		router.HandleFunc("/dial", dialHandler).Methods("POST")
		router.HandleFunc("/health", healthcheckHandler)
		server := &http.Server{
			Addr:    s.HTTPSAddress,
			Handler: router,
			TLSConfig: &tls.Config{
				ClientAuth: tls.RequireAndVerifyClientCert,
				ClientCAs:  pool,
			},
		}
		return server.ListenAndServeTLS(s.Cert, s.Key)
	})

	g.Go(func() error {
		router := http.NewServeMux()
		router.HandleFunc("/health", healthcheckHandler)
		return http.ListenAndServe(s.HTTPAddress, router)
	})

	if err = g.Wait(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}
