package compound

import (
	"archive/zip"
	"context"
	"flag"
	"os"
	"path"
	"testing"

	"github.com/google/subcommands"
	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/fileblob"
	"gopkg.in/yaml.v3"

	downloadbucket "github.com/hazelcast/platform-operator-agent/init/jar_download_bucket"
	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
)

func Test_Execute_BundleCommand(t *testing.T) {
	dir := t.TempDir()
	conigF, err := os.CreateTemp(dir, "config.yaml")
	require.Nil(t, err)
	bucketPath := path.Join(dir, "bucket")
	err = fileutil.CreateFiles(bucketPath, []fileutil.File{{Name: "my-jar.jar", IsDir: false}}, true)
	require.Nil(t, err)
	zipFile := path.Join(dir, "namespace1.zip")
	cfg := &ConfigWrapper{
		InitContainer: &Config{
			Download: &Download{
				Bundle: &Bundle{
					Buckets: []downloadbucket.Cmd{
						{
							Destination: zipFile,
							BucketURI:   "file://" + bucketPath,
						},
					},
				},
			},
		},
	}
	cfgData, err := yaml.Marshal(cfg)
	require.Nil(t, err)
	err = os.WriteFile(conigF.Name(), cfgData, os.FileMode(0755))
	require.Nil(t, err)

	cmd := Cmd{ConfigFileLocation: conigF.Name()}
	exStatus := cmd.Execute(context.TODO(), &flag.FlagSet{})
	require.Equal(t, subcommands.ExitSuccess, exStatus)
	zf, err := zip.OpenReader(zipFile)
	require.Nil(t, err)
	defer zf.Close()
	require.Equal(t, 1, len(zf.File))
}
