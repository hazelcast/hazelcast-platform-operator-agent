package restore

import "github.com/hazelcast/platform-operator-agent/internal"

var ExampleTarGzFiles = []internal.File{
	{"cluster", true},
	{"cluster/cluster-state.txt", false},
	{"cluster/cluster-version.txt", false},
	{"cluster/partition-thread-count.bin", false},
	{"configs", true},
	{"s00", true},
	{"s00/tombstone", true},
	{"cluster/members.bin", false},
	{"s00/tombstone/02", true},
	{"s00/tombstone/02/0000000000000002.chunk", false},
	{"s00/value", true},
	{"s00/value/01", true},
	{"s00/value/01/0000000000000001.chunk", false},
}
