package usercode_url

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
	"github.com/hazelcast/platform-operator-agent/internal/logger"
)

const usercodeLock = "usercode_url_lock"

var log = logger.New().Named("user_code_url")

type Cmd struct {
	URLs        string `envconfig:"UC_URL_URLS"`
	Destination string `envconfig:"UC_URL_DESTINATION"`
}

func (*Cmd) Name() string     { return "user-code-url" }
func (*Cmd) Synopsis() string { return "Run User Code URL Agent" }
func (*Cmd) Usage() string    { return "" }

func (r *Cmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	f.StringVar(&r.URLs, "urls", "", "comma separated urls")
	f.StringVar(&r.Destination, "dst", "/opt/hazelcast/userCode/urls", "dst filesystem path")
}

func (r *Cmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Info("starting user code url agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("uc_url", r); err != nil {
		log.Error("an error occurred while processing config from env: " + err.Error())
		return subcommands.ExitFailure
	}

	lock := filepath.Join(r.Destination, usercodeLock)
	if _, err := os.Stat(lock); err == nil || os.IsExist(err) {
		// If usercodeLock lock exists exit
		log.Error("lock file exists, exiting: " + err.Error())
		return subcommands.ExitSuccess
	}

	urls := strings.Split(r.URLs, ",")

	// run download process
	log.Info("starting download", zap.String("destination", r.Destination))
	if err := downloadFiles(ctx, urls, r.Destination); err != nil {
		log.Error("download error: " + err.Error())
		return subcommands.ExitFailure
	}

	if err := os.WriteFile(lock, []byte{}, 0600); err != nil {
		log.Error("lock file creation error: " + err.Error())
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

func downloadFiles(ctx context.Context, srcURLs []string, dst string) error {
	g, groupCtx := errgroup.WithContext(ctx)
	for _, url := range srcURLs {
		url := url
		g.Go(func() error {
			return fileutil.DownloadFileFromURL(groupCtx, url, dst)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
