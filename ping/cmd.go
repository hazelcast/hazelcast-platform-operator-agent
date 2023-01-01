package ping

import (
	"context"
	"crypto/x509"
	"flag"
	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/sync/errgroup"
	"log"
	"os"
)

type Cmd struct {
	HTTPAddress  string `envconfig:"BACKUP_HTTP_ADDRESS"`
	HTTPSAddress string `envconfig:"BACKUP_HTTPS_ADDRESS"`
	CA           string `envconfig:"BACKUP_CA"`
	Cert         string `envconfig:"BACKUP_CERT"`
	Key          string `envconfig:"BACKUP_KEY"`
}

func (*Cmd) Name() string     { return "ping" }
func (*Cmd) Synopsis() string { return "run ping sidecar service" }
func (*Cmd) Usage() string    { return "" }

func (p *Cmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&p.HTTPAddress, "http-address", ":8080", "http server listen address")
	f.StringVar(&p.HTTPSAddress, "https-address", ":8443", "https server listen address")
	f.StringVar(&p.CA, "ca", "ca.crt", "http server client ca")
	f.StringVar(&p.Cert, "cert", "tls.crt", "http server tls cert")
	f.StringVar(&p.Key, "key", "tls.key", "http server tls key")
}

func (p *Cmd) Execute(_ context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Println("Starting ping agent")

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

	var g errgroup.Group
	g.Go(RunServerWithTLS(p, pool))
	g.Go(RunHealthCheckServer(p))

	if err = g.Wait(); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}
