package compound

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/hazelcast/platform-operator-agent/internal/logger"
)

var log = logger.New().Named("compound_command")

type Cmd struct {
	ConfigFileLocation string `envconfig:"CONFIG_FILE"`
}

func (c *Cmd) Name() string {
	return "execute-multiple-commands"
}
func (c *Cmd) Synopsis() string {
	return "Reads the provided configuration and executes all defined commands"
}
func (c *Cmd) Usage() string {
	return ""
}

func (c *Cmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&c.ConfigFileLocation, "config-file-location", "", "Location of the YAML Config file")
}

func (c *Cmd) Execute(ctx context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	// overwrite config with environment variables
	if err := envconfig.Process("config", c); err != nil {
		log.Error("an error occurred while processing config from env: " + err.Error())
		return subcommands.ExitFailure
	}

	data, err := os.ReadFile(c.ConfigFileLocation)
	cfg := &ConfigWrapper{}
	if err = yaml.Unmarshal(data, cfg); err != nil {
		log.Error("Unable to unmarshal YAML config data: " + err.Error())
		return subcommands.ExitFailure
	}
	if cfg.InitContainer == nil {
		log.Info("Not initContainer config provided.")
		return subcommands.ExitSuccess
	}
	g := new(errgroup.Group)
	g.Go(func() error {
		return executeDownloadCommands(ctx, cfg.InitContainer.Download, f, args)
	})
	g.Go(func() error {
		return executeRestoreCommands(ctx, cfg.InitContainer.Restore, f, args)
	})
	if err := g.Wait(); err != nil {
		log.Error("error during execution: " + err.Error())
	}
	time.Sleep(1000 * time.Second)
	return subcommands.ExitSuccess
}

func executeDownloadCommands(ctx context.Context, d *Download, f *flag.FlagSet, args ...interface{}) error {
	if d == nil {
		return nil
	}
	if d.Bucket != nil {
		if s := d.Bucket.Execute(ctx, f, args); s != subcommands.ExitSuccess {
			return fmt.Errorf("error executing bucket download command")
		}
	}
	if d.URL != nil {
		if s := d.URL.Execute(ctx, f, args); s != subcommands.ExitSuccess {
			return fmt.Errorf("error executing URL download command")
		}
	}
	return nil
}

func executeRestoreCommands(ctx context.Context, r *Restore, f *flag.FlagSet, args ...interface{}) error {
	if r == nil {
		return nil
	}
	if r.Bucket != nil {
		if s := r.Bucket.Execute(ctx, f, args); s != subcommands.ExitSuccess {
			return fmt.Errorf("error executing bucket restore command")
		}
	}
	if r.PVC != nil {
		if s := r.PVC.Execute(ctx, f, args); s != subcommands.ExitSuccess {
			return fmt.Errorf("error executing PVC restore command")
		}
	}
	return nil
}
