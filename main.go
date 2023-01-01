package main

import (
	"context"
	"flag"
	"github.com/google/subcommands"
	"github.com/hazelcast/platform-operator-agent/backup"
	"github.com/hazelcast/platform-operator-agent/ping"
	"github.com/hazelcast/platform-operator-agent/restore"
	"github.com/hazelcast/platform-operator-agent/usercode"
	"os"
)

func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")

	subcommands.Register(&backup.Cmd{}, "")
	subcommands.Register(&usercode.Cmd{}, "")
	subcommands.Register(&restore.BucketToHostpathCmd{}, "")
	subcommands.Register(&restore.LocalInHostpathCmd{}, "")
	subcommands.Register(&restore.LocalInPVCCmd{}, "")
	subcommands.Register(&restore.BucketToPVCCmd{}, "")
	subcommands.Register(&ping.Cmd{}, "")

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
