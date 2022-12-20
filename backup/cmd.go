package backup

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/google/subcommands"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/sync/errgroup"
)

const (
	DirName = "hot-backup"
)

type Cmd struct {
	HTTPAddress  string `envconfig:"BACKUP_HTTP_ADDRESS"`
	HTTPSAddress string `envconfig:"BACKUP_HTTPS_ADDRESS"`
	CA           string `envconfig:"BACKUP_CA"`
	Cert         string `envconfig:"BACKUP_CERT"`
	Key          string `envconfig:"BACKUP_KEY"`
}

func (*Cmd) Name() string     { return "backup" }
func (*Cmd) Synopsis() string { return "run backup sidecar service" }
func (*Cmd) Usage() string    { return "" }

func (p *Cmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&p.HTTPAddress, "http-address", ":8080", "http server listen address")
	f.StringVar(&p.HTTPSAddress, "https-address", ":8443", "https server listen address")
	f.StringVar(&p.CA, "ca", "ca.crt", "http server client ca")
	f.StringVar(&p.Cert, "cert", "tls.crt", "http server tls cert")
	f.StringVar(&p.Key, "key", "tls.key", "http server tls key")
}

func (p *Cmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Println("Starting backup agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("backup", p); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	ca, err := os.ReadFile(p.CA)
	if err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(ca); !ok {
		log.Println("failed to find any PEM data in ca input")
		return subcommands.ExitFailure
	}

	s := service{
		tasks: make(map[uuid.UUID]*task),
	}

	var g errgroup.Group
	g.Go(func() error {
		router := mux.NewRouter().StrictSlash(true)
		router.HandleFunc("/backup", s.listBackupsHandler).Methods("GET")
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

	if err = g.Wait(); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}
