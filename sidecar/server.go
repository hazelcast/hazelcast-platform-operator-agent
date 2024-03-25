package sidecar

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"

	"github.com/hazelcast/platform-operator-agent/internal/logger"
)

var serverLog = logger.New().Named("server")

func startServer(ctx context.Context, s *Cmd) error {
	ca, err := os.ReadFile(s.CA)
	if err != nil {
		serverLog.Error("error while reading CA: " + err.Error())
		return err
	}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(ca); !ok {
		err = fmt.Errorf("failed to find any PEM data in ca input")
		serverLog.Error(err.Error())
		return err
	}

	backupService := Service{
		Tasks: make(map[uuid.UUID]*task),
	}

	dialService := DialService{}

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		router := mux.NewRouter().StrictSlash(true)
		router.HandleFunc("/backup", backupService.listBackupsHandler).Methods("GET")
		router.HandleFunc("/upload", backupService.uploadHandler).Methods("POST")
		router.HandleFunc("/upload/{id}", backupService.statusHandler).Methods("GET")
		router.HandleFunc("/upload/{id}/cancel", backupService.cancelHandler).Methods("POST")
		router.HandleFunc("/upload/{id}/cleanup", backupService.cleanupHandler).Methods("POST")
		router.HandleFunc("/upload/{id}", backupService.deleteHandler).Methods("DELETE")
		router.HandleFunc("/download", backupService.downloadFileHandler).Methods("POST")
		router.HandleFunc("/bundle", backupService.bundleHandler).Methods("POST")
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
		serverLog.Error("an error occurred while setting up server: " + err.Error())
		return err
	}

	return nil
}
