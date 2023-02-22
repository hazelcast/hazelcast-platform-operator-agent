package sidecar

import (
	"context"
	"flag"

	"github.com/google/subcommands"
	"github.com/hazelcast/platform-operator-agent/internal/logger"
	"github.com/kelseyhightower/envconfig"
)

var cmdLog = logger.New().Named("cmd")

type Cmd struct {
	HTTPAddress  string `envconfig:"BACKUP_HTTP_ADDRESS"`
	HTTPSAddress string `envconfig:"BACKUP_HTTPS_ADDRESS"`
	CA           string `envconfig:"BACKUP_CA"`
	Cert         string `envconfig:"BACKUP_CERT"`
	Key          string `envconfig:"BACKUP_KEY"`
}

func (*Cmd) Name() string     { return "sidecar" }
func (*Cmd) Synopsis() string { return "run sidecar service" }
func (*Cmd) Usage() string    { return "" }

func (p *Cmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&p.HTTPAddress, "http-address", ":8080", "http server listen address")
	f.StringVar(&p.HTTPSAddress, "https-address", ":8443", "https server listen address")
	f.StringVar(&p.CA, "ca", "ca.crt", "http server client ca")
	f.StringVar(&p.Cert, "cert", "tls.crt", "http server tls cert")
	f.StringVar(&p.Key, "key", "tls.key", "http server tls key")
}

func (p *Cmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	cmdLog.Info("starting sidecar agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("sidecar", p); err != nil {
		cmdLog.Error("an error occurred while processing config from env: " + err.Error())
		return subcommands.ExitFailure
	}

	err := startServer(ctx, p)
	if err != nil {
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
