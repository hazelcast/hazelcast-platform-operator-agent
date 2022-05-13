package util

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func ZipFolder(sourceFolder, destinationFile string) error {
	outFile, outFileErr := os.Create(destinationFile)
	if outFileErr != nil {
		return fmt.Errorf("couldn't create zip file %s. Err: %v", destinationFile, outFileErr)
	}
	defer outFile.Close()

	w := zip.NewWriter(outFile)
	addFilesErr := addFiles(w, sourceFolder, "")
	if addFilesErr != nil {
		return fmt.Errorf("couldn't add files. Err: %v", addFilesErr)
	}
	closeErr := w.Close()
	if closeErr != nil {
		return fmt.Errorf("couldn't close writer. Err: %v", closeErr)
	}
	return nil
}

func addFiles(w *zip.Writer, basePath, baseInZip string) error {
	return filepath.Walk(basePath, func(fullpath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		path := strings.TrimPrefix(fullpath, basePath)
		if path == "" {
			return nil
		}

		// make sure our path is relative to baseInZip
		path = filepath.Join(baseInZip, path)

		if info.IsDir() {
			// directories always end with path separator
			if _, err := w.Create(path + string(os.PathSeparator)); err != nil {
				return err
			}
			return nil
		}

		// add file to archive
		s, err := os.Open(fullpath)
		if err != nil {
			return err
		}
		defer s.Close()

		d, err := w.Create(path)
		if err != nil {
			return err
		}

		if _, err = io.Copy(d, s); err != nil {
			return err
		}

		return nil
	})
}
