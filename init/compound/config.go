package compound

import (
	downloadurl "github.com/hazelcast/platform-operator-agent/init/file_download_url"
	downloadbucket "github.com/hazelcast/platform-operator-agent/init/jar_download_bucket"
	"github.com/hazelcast/platform-operator-agent/init/restore"
)

type ConfigWrapper struct {
	InitContainer *Config `yaml:"initContainer,omitempty"`
}

type Config struct {
	Download *Download `yaml:"download,omitempty"`
	Restore  *Restore  `yaml:"restore,omitempty"`
}

type Download struct {
	Buckets []downloadbucket.Cmd `yaml:"buckets,omitempty"`
	URLs    []downloadurl.Cmd    `yaml:"urls,omitempty"`
	Bundle  *Bundle              `yaml:"bundle,omitempty"`
}

type Bundle struct {
	Buckets []downloadbucket.Cmd `yaml:"buckets,omitempty"`
}

type Restore struct {
	Bucket *restore.BucketToPVCCmd `yaml:"bucket,omitempty"`
	PVC    *restore.LocalInPVCCmd  `yaml:"pvc,omitempty"`
}
