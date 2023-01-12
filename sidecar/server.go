package sidecar

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"
)

func startServer(s *Cmd) error {
	ca, err := os.ReadFile(s.CA)
	if err != nil {
		s.Logger.Error(err, "error while reading CA")
		return err
	}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(ca); !ok {
		err = fmt.Errorf("failed to find any PEM data in ca input")
		s.Logger.Error(err, "")
		return err
	}

	backupService := Service{
		Tasks:  make(map[uuid.UUID]*task),
		Logger: s.Logger,
	}

	dialService := DialService{Logger: s.Logger}

	var g errgroup.Group
	g.Go(func() error {
		router := mux.NewRouter().StrictSlash(true)
		router.HandleFunc("/backup", backupService.listBackupsHandler).Methods("GET")
		router.HandleFunc("/upload", backupService.uploadHandler).Methods("POST")
		router.HandleFunc("/upload/{id}", backupService.statusHandler).Methods("GET")
		router.HandleFunc("/upload/{id}", backupService.cancelHandler).Methods("DELETE")
		router.HandleFunc("/dial", dialService.dialHandler).Methods("POST")
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
		s.Logger.Error(err, "an error occurred while setting up server")
		return err
	}

	return nil
}
