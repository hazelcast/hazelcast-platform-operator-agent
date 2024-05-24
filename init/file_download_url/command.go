package downloadurl

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

const urlFileLock = ".file_download_url"

var log = logger.New().Named("file_download_url")

type Cmd struct {
	Destination string `envconfig:"FDU_DESTINATION" yaml:"destination"`
	URLs        string `envconfig:"FDU_URLS" yaml:"urls"`
}

func (*Cmd) Name() string     { return "file-download-url" }
func (*Cmd) Synopsis() string { return "Run File Download from URL Agent" }
func (*Cmd) Usage() string    { return "" }

func (r *Cmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	f.StringVar(&r.URLs, "urls", "", "comma separated urls")
	f.StringVar(&r.Destination, "dst", "", "dst filesystem path")
}

func (r *Cmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// overwrite config with environment variables
	if err := envconfig.Process("fdu", r); err != nil {
		log.Error("an error occurred while processing config from env: " + err.Error())
		return subcommands.ExitFailure
	}

	lock := filepath.Join(r.Destination, urlFileLock)
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
