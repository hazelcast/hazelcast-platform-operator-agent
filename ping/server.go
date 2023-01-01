package ping

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/go-ping/ping"
	"github.com/gorilla/mux"
	"github.com/hazelcast/platform-operator-agent/internal/serverutil"
	"log"
	"net/http"
)

func RunHealthCheckServer(p *Cmd) func() error {
	return func() error {
		router := http.NewServeMux()
		router.HandleFunc("/health", healthcheckHandler)
		return http.ListenAndServe(p.HTTPAddress, router)
	}
}

func RunServerWithTLS(p *Cmd, pool *x509.CertPool) func() error {
	return func() error {
		router := mux.NewRouter().StrictSlash(true)
		router.HandleFunc("/ping", pingHandler).Methods("POST")
		router.HandleFunc("/health", healthcheckHandler)
		server := &http.Server{
			Addr:    p.HTTPSAddress,
			Handler: router,
			TLSConfig: &tls.Config{
				ClientAuth: tls.RequireAndVerifyClientCert,
				ClientCAs:  pool,
			},
		}
		return server.ListenAndServe()
	}
}

type Request struct {
	Endpoints string `json:"endpoints"`
}

type Response struct {
	Success bool `json:"success"`
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	var req Request
	if err := serverutil.DecodeBody(r, &req); err != nil {
		log.Println("PING", "Error occurred while parsing body:", err)
		serverutil.HttpError(w, http.StatusBadRequest)
		return
	}

	pinger, err := ping.NewPinger(req.Endpoints)
	if err != nil {
		panic(err)
	}
	pinger.Count = 3
	err = pinger.Run() // Blocks until finished.
	if err != nil {
		panic(err)
	}
	stats := pinger.Statistics() // get send/receive/duplicate/rtt stats
	if stats.PacketLoss == float64(0) {
		serverutil.HttpJSON(w, Response{Success: true})
	} else {
		serverutil.HttpJSON(w, Response{Success: false})
	}
}

func healthcheckHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
