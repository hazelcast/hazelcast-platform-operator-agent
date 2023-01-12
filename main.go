package main

import (
	"context"
	"flag"
	"github.com/hazelcast/platform-operator-agent/internal/logger"
	"os"

	"github.com/google/subcommands"
	"github.com/hazelcast/platform-operator-agent/init/restore"
	"github.com/hazelcast/platform-operator-agent/init/usercode"
	"github.com/hazelcast/platform-operator-agent/sidecar"
)

func main() {
	log, err := logger.New()
	if err != nil {
		panic(err)
	}

	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")

	subcommands.Register(&usercode.Cmd{Logger: log.WithName("user code deployment")}, "")
	subcommands.Register(&restore.BucketToHostpathCmd{Logger: log.WithName("restore from bucket to hostpath")}, "")
	subcommands.Register(&restore.LocalInHostpathCmd{Logger: log.WithName("restore from Local in hostpath")}, "")
	subcommands.Register(&restore.LocalInPVCCmd{Logger: log.WithName("restore from local in PVC")}, "")
	subcommands.Register(&restore.BucketToPVCCmd{Logger: log.WithName("restore from bucket to PVC")}, "")
	subcommands.Register(&sidecar.Cmd{Logger: log.WithName("sidecar")}, "")

	flag.Parse()

	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
