package restore

import (
	"context"
	"flag"
	"fmt"
	"github.com/hazelcast/platform-operator-agent/sidecar"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/google/subcommands"
	"github.com/kelseyhightower/envconfig"

	"github.com/hazelcast/platform-operator-agent/internal/fileutil"

	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

type LocalInPVCCmd struct {
	BackupSequenceFolderName string `envconfig:"RESTORE_LOCAL_BACKUP_FOLDER_NAME"`
	BackupBaseDir            string `envconfig:"RESTORE_LOCAL_BACKUP_BASE_DIR"`
	Hostname                 string `envconfig:"RESTORE_LOCAL_HOSTNAME"`
	RestoreID                string `envconfig:"RESTORE_LOCAL_ID"`
}

func (*LocalInPVCCmd) Name() string     { return "restore_pvc_local" }
func (*LocalInPVCCmd) Synopsis() string { return "run restore pvc local agent" }
func (*LocalInPVCCmd) Usage() string    { return "" }

func (r *LocalInPVCCmd) SetFlags(f *flag.FlagSet) {
	// We ignore error because this is just a default value
	hostname, _ := os.Hostname()
	f.StringVar(&r.Hostname, "hostname", hostname, "dst filesystem path")
	f.StringVar(&r.BackupSequenceFolderName, "src", "", "src backup folder path")
	f.StringVar(&r.BackupBaseDir, "dst", "/data/persistence/backup", "dst filesystem path")
	f.StringVar(&r.RestoreID, "restore-id", "", "Restore ID for which the lock will be created.")
}

func (r *LocalInPVCCmd) Execute(_ context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	log.Println("Starting restore pvc local agent...")

	// overwrite config with environment variables
	if err := envconfig.Process("restoreLocal", r); err != nil {
		log.Println(err)
		return subcommands.ExitFailure
	}

	if !hostnameRE.MatchString(r.Hostname) {
		log.Println("Invalid hostname, need to conform to statefulset naming scheme")
		return subcommands.ExitFailure
	}

	id, err := parseID(r.Hostname)
	if err != nil {
		log.Println("Parse error", err.Error())
		return subcommands.ExitFailure
	}

	lock := filepath.Join(r.BackupBaseDir, lockFileName(r.RestoreID, id))

	if _, err := os.Stat(lock); err == nil || os.IsExist(err) {
		// If restoreLocal lock exists exit
		log.Println("Restore lock exists, exiting")
		return subcommands.ExitSuccess
	}

	err = copyBackupPVC(path.Join(r.BackupBaseDir, sidecar.DirName, r.BackupSequenceFolderName), r.BackupBaseDir)
	if err != nil {
		log.Println("Copy backup failed", err)
		return subcommands.ExitFailure
	}

	if err := cleanupLocks(r.BackupBaseDir, id); err != nil {
		log.Println("Error cleaning up locks", err)
		return subcommands.ExitFailure
	}

	if err := os.WriteFile(lock, []byte{}, 0600); err != nil {
		log.Println("Lock file creation error", err)
		return subcommands.ExitFailure
	}

	log.Println("Restore successful")
	return subcommands.ExitSuccess
}

func copyBackupPVC(backupDir, destDir string) error {
	backupUUIDs, err := fileutil.FolderUUIDs(backupDir)
	if err != nil {
		return err
	}

	if len(backupUUIDs) != 1 {
		return fmt.Errorf("incorrect number of backups %d in backup sequence folder", len(backupUUIDs))
	}

	destBackupUUIDS, err := fileutil.FolderUUIDs(destDir)
	if err != nil {
		return err
	}

	// Remove the hot-restart folder at the destination
	for _, uuid := range destBackupUUIDS {
		err = os.RemoveAll(path.Join(destDir, uuid.Name()))
		if err != nil {
			return err
		}
	}

	bk := backupUUIDs[0].Name()
	return copyDir(path.Join(backupDir, bk), path.Join(destDir, bk))
}
