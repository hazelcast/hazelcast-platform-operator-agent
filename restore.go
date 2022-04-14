package main

import (
	"context"
	"flag"
	"log"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
)

type restoreCmd struct {
	// credentials from secret
	Region       string
	AccessKey    string
	AccessSecret string
	// backup to restore
	BucketPath string
}

func (*restoreCmd) Name() string     { return "restore" }
func (*restoreCmd) Synopsis() string { return "run restore agent" }
func (*restoreCmd) Usage() string    { return "" }

func (r *restoreCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&r.Region, "region", "", "bucket cloud region")
	f.StringVar(&r.AccessKey, "key", "", "bucket access key")
	f.StringVar(&r.AccessSecret, "secret", "", "bucket secret key")
	f.StringVar(&r.BucketPath, "path", "", "bucket path")
}

func (r *restoreCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// overwrite config with environment variables
	if err := envconfig.Process("restore", r); err != nil {
		log.Fatalln(err)
		return subcommands.ExitUsageError
	}

	return subcommands.ExitSuccess
}
