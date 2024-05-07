package compound

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/hazelcast/platform-operator-agent/internal/bucket"
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
	if err != nil {
		log.Error("Unable to read YAML config file: " + err.Error())
		return subcommands.ExitFailure
	}
	cfg := &ConfigWrapper{}
	if err = yaml.Unmarshal(data, cfg); err != nil {
		log.Error("Unable to unmarshal YAML config data: " + err.Error())
		return subcommands.ExitFailure
	}
	if cfg.InitContainer == nil {
		log.Info("No initContainer config provided.")
		return subcommands.ExitSuccess
	}

	if err = createLiteMemberEnvVar(cfg.InitContainer.LiteMemberCount); err != nil {
		log.Error("error during creating lite member env var: " + err.Error())
		return subcommands.ExitFailure
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
	log.Info("Successfully executed compound command")
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
	if d.Bundle != nil {
		g := new(errgroup.Group)
		for _, cmd := range d.Bundle.Buckets {
			cmd := cmd
			g.Go(func() error {
				log.Info("Download bundle for " + cmd.Destination)
				return bucket.DownloadBundle(ctx, bucket.BundleReq{
					URL:        cmd.BucketURI,
					SecretName: cmd.SecretName,
					DestDir:    cmd.Destination,
				})
			})
		}
		return g.Wait()
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

// createLiteMemberEnvVar creates a file consist of HZ_LITEMEMBER_ENABLED environment variable
func createLiteMemberEnvVar(liteMemberCount int) error {
	re := regexp.MustCompile(`\d+$`) // regex for hostname, such as hazelcast-0
	match := re.FindString(os.Getenv("HOSTNAME"))
	index, err := strconv.Atoi(match)
	if err != nil {
		return err
	}
	var envFile *os.File
	var liteMemberEnvVar string
	if index < liteMemberCount { // if lite member count is 1, then it means hazelcast-0 is lite member
		envFile, err = os.Create("/tmp/env_vars")
		if err != nil {
			return err
		}
		defer envFile.Close()
		liteMemberEnvVar = "HZ_LITEMEMBER_ENABLED=true"
	} else {
		liteMemberEnvVar = "HZ_LITEMEMBER_ENABLED=false"
	}
	_, err = envFile.WriteString(liteMemberEnvVar)
	if err != nil {
		return err
	}
	return nil
}
