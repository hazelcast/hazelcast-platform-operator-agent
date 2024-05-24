package compound

import (
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
	err = fileutil.CreateFiles(bucketPath, []fileutil.File{{"my-jar.jar", false}}, true)
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
	_, err = os.Stat(zipFile)
	require.Nil(t, err)
}
