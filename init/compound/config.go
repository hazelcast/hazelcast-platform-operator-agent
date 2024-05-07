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
	Download        *Download `yaml:"download,omitempty"`
	Restore         *Restore  `yaml:"restore,omitempty"`
	LiteMemberCount int       `yaml:"liteMemberCount"`
}

type Download struct {
	Bucket *downloadbucket.Cmd `yaml:"bucket,omitempty"`
	URL    *downloadurl.Cmd    `yaml:"url,omitempty"`
	Bundle *Bundle             `yaml:"bundle,omitempty"`
}

type Bundle struct {
	Buckets []downloadbucket.Cmd `yaml:"buckets,omitempty"`
}

type Restore struct {
	Bucket *restore.BucketToPVCCmd `yaml:"bucket,omitempty"`
	PVC    *restore.LocalInPVCCmd  `yaml:"pvc,omitempty"`
}
