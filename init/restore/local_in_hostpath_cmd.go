package restore

import (
	"context"
	"flag"
	"fmt"
	"github.com/go-logr/logr"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
	"github.com/hazelcast/platform-operator-agent/sidecar"
)

type LocalInHostpathCmd struct {
	BackupFolderName string `envconfig:"RESTORE_LOCAL_BACKUP_FOLDER_NAME"`
	BackupBaseDir    string `envconfig:"RESTORE_LOCAL_BACKUP_BASE_DIR"`
	Hostname         string `envconfig:"RESTORE_LOCAL_HOSTNAME"`
	RestoreID        string `envconfig:"RESTORE_LOCAL_ID"`
	Logger           logr.Logger
}

func (*LocalInHostpathCmd) Name() string     { return "restore_hostpath_local" }
func (*LocalInHostpathCmd) Synopsis() string { return "run restore hostpath local agent" }
func (*LocalInHostpathCmd) Usage() string    { return "" }

func (r *LocalInHostpathCmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	hostname, _ := os.Hostname()
	f.StringVar(&r.Hostname, "hostname", hostname, "dst filesystem path")
	f.StringVar(&r.BackupFolderName, "src", "", "src backup folder path")
	f.StringVar(&r.BackupBaseDir, "dst", "/data/persistence/backup", "dst filesystem path")
	f.StringVar(&r.RestoreID, "restore-id", "", "Restore ID for which the lock will be created.")
}

func (r *LocalInHostpathCmd) Execute(_ context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	r.Logger.Info("starting restore hostpath local agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("restoreLocal", r); err != nil {
		r.Logger.Error(err, "an error occurred while processing config from env")
		return subcommands.ExitFailure
	}

	if !hostnameRE.MatchString(r.Hostname) {
		r.Logger.Error(fmt.Errorf("need to conform to statefulset naming scheme"), "invalid hostname")
		return subcommands.ExitFailure
	}

	id, err := parseID(r.Hostname)
	if err != nil {
		r.Logger.Error(err, "parse error")
		return subcommands.ExitFailure
	}

	lock := filepath.Join(r.BackupBaseDir, lockFileName(r.RestoreID, id))

	if _, err = os.Stat(lock); err == nil || os.IsExist(err) {
		// If restoreLocal lock exists exit
		r.Logger.Info("restore lock exists, exiting")
		return subcommands.ExitSuccess
	}

	err = copyBackup(r.Logger, path.Join(r.BackupBaseDir, sidecar.DirName, r.BackupFolderName), r.BackupBaseDir, id)
	if err != nil {
		r.Logger.Error(err, "copy backup failed")
		return subcommands.ExitFailure
	}

	if err = cleanupLocks(r.BackupBaseDir, id); err != nil {
		r.Logger.Error(err, "error cleaning up locks")
		return subcommands.ExitFailure
	}

	if err = os.WriteFile(lock, []byte{}, 0600); err != nil {
		r.Logger.Error(err, "lock file creation error")
		return subcommands.ExitFailure
	}

	r.Logger.Info("restore successful")
	return subcommands.ExitSuccess
}

func copyBackup(logger logr.Logger, backupDir, destDir string, id int) error {
	backupUUIDs, err := fileutil.FolderUUIDs(backupDir)
	if err != nil {
		return err
	}

	if len(backupUUIDs) != 1 && len(backupUUIDs) <= id {
		return fmt.Errorf("backup id is out of range")
	}

	// If there is only one backup, members are isolated. No need for memberID
	if len(backupUUIDs) == 1 {
		id = 0
	}

	destBackupUUIDS, err := fileutil.FolderUUIDs(destDir)
	if err != nil {
		return err
	}

	if len(backupUUIDs) != len(destBackupUUIDS) {
		return fmt.Errorf("local backup count %d and hot-restart folder %d count are not equal", len(backupUUIDs), len(destBackupUUIDS))
	}

	if backupUUIDs[id].Name() != destBackupUUIDS[id].Name() {
		logger.Info(fmt.Sprintf("hot-restart folder name %s and backup UUID folder are not the same %s", destBackupUUIDS[id].Name(), backupUUIDs[id].Name()))
	}

	bk := backupUUIDs[id].Name()
	// Remove the hot-restart folder at the destination
	err = os.RemoveAll(path.Join(destDir, destBackupUUIDS[id].Name()))
	if err != nil {
		return err
	}
	return copyDir(path.Join(backupDir, bk), path.Join(destDir, bk))
}

func copyDir(source, destination string) error {
	var err = filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		var out = filepath.Join(destination, strings.TrimPrefix(path, source))

		if info.IsDir() {
			return os.Mkdir(filepath.Join(out), info.Mode())
		}
		err = func() error {
			in, err := os.Open(path)
			if err != nil {
				return err
			}
			defer in.Close()

			// create output
			fh, err := os.Create(out)
			if err != nil {
				return err
			}
			defer fh.Close()

			// change file mode
			err = fh.Chmod(info.Mode())
			if err != nil {
				return err
			}

			// copy content
			_, err = io.Copy(fh, in)
			return err
		}()

		return err

	})
	return err
}

func lockFileName(restoreId string, memberId int) string {
	return fmt.Sprintf(".%s.%s.%d", restoreLock, restoreId, memberId)
}
