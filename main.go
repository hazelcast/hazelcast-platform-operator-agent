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

	subcommands.Register(&usercode.Cmd{Logger: log.WithName("User Code Deployment")}, "")
	subcommands.Register(&restore.BucketToHostpathCmd{Logger: log.WithName("Restore from Bucket to Hostpath")}, "")
	subcommands.Register(&restore.LocalInHostpathCmd{Logger: log.WithName("Restore from Local in Hostpath")}, "")
	subcommands.Register(&restore.LocalInPVCCmd{Logger: log.WithName("Restore from Local in PVC")}, "")
	subcommands.Register(&restore.BucketToPVCCmd{Logger: log.WithName("Restore from Bucket to PVC")}, "")
	subcommands.Register(&sidecar.Cmd{Logger: log.WithName("Sidecar")}, "")

	flag.Parse()

	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
