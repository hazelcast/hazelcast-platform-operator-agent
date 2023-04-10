package main

import (
	"context"
	"flag"
	"os"

	"github.com/google/subcommands"

	"github.com/hazelcast/platform-operator-agent/init/restore"
	"github.com/hazelcast/platform-operator-agent/init/usercode_bucket"
	"github.com/hazelcast/platform-operator-agent/init/usercode_url"
	"github.com/hazelcast/platform-operator-agent/sidecar"
)

func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")

	subcommands.Register(&usercode_bucket.Cmd{}, "")
	subcommands.Register(&usercode_url.Cmd{}, "")
	subcommands.Register(&restore.LocalInPVCCmd{}, "")
	subcommands.Register(&restore.BucketToPVCCmd{}, "")
	subcommands.Register(&sidecar.Cmd{}, "")

	flag.Parse()

	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
