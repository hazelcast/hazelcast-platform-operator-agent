package main

import (
	"context"
	"flag"
	"os"

	"github.com/google/subcommands"

	"github.com/hazelcast/platform-operator-agent/init/compound"
	downloadurl "github.com/hazelcast/platform-operator-agent/init/file_download_url"
	downloadbucket "github.com/hazelcast/platform-operator-agent/init/jar_download_bucket"
	"github.com/hazelcast/platform-operator-agent/init/restore"
	"github.com/hazelcast/platform-operator-agent/sidecar"
)

func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")

	subcommands.Register(&compound.Cmd{}, "")
	subcommands.Register(&downloadurl.Cmd{}, "")
	subcommands.Register(&downloadbucket.Cmd{}, "")
	subcommands.Register(&restore.LocalInPVCCmd{}, "")
	subcommands.Register(&restore.BucketToPVCCmd{}, "")
	subcommands.Register(&sidecar.Cmd{}, "")

	flag.Parse()

	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
