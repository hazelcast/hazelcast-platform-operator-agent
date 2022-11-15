package main

import (
	"context"
	"flag"
	"os"

	"github.com/google/subcommands"

	agent "github.com/hazelcast/platform-operator-agent"
)

func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&agent.BackupCmd{}, "")
	subcommands.Register(&agent.RestoreCmd{}, "")
	subcommands.Register(&agent.UserCodeDeploymentCmd{}, "")
	subcommands.Register(&agent.RestoreLocalCmd{}, "")

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
