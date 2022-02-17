package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// ConvertHumanReadableFormat converts backup-sequenceID into human readable format.
// backup-1643801670242 --> 2022-02-18-14-57-44
func ConvertHumanReadableFormat(backupFolderName string) string{
	epochString := strings.ReplaceAll(backupFolderName, "backup-", "")
	timestamp, _ := strconv.ParseInt(epochString, 10, 64)
	t := time.Unix(0, timestamp*int64(time.Millisecond))
	return t.Format("2006-01-02-15-04-05")
}

func RemoveAllContent(folder string) error{
	content, readErr := ioutil.ReadDir(folder)
	if readErr != nil {
		return fmt.Errorf("Unable to read content of folder %s to remove. Err: %v", folder, readErr)
	}
	for _, subFolder := range content {
		if removeErr := os.RemoveAll(path.Join(folder, subFolder.Name())); removeErr != nil {
			return fmt.Errorf("Unable to delete folder %s. Err: %v", subFolder.Name(), removeErr)
		}
	}
	return nil
}