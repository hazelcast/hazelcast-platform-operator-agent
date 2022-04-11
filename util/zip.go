package util

import (
	"archive/zip"
	"fmt"
	"io/ioutil"
	"os"
	"path"
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
	files, filesErr := ioutil.ReadDir(basePath)
	if filesErr != nil {
		return fmt.Errorf("couldn't read %s. Err: %v", basePath, filesErr)
	}

	for _, file := range files {
		if !file.IsDir() {
			fileName := path.Join(basePath, file.Name())
			data, dataErr := ioutil.ReadFile(fileName)
			if dataErr != nil {
				return fmt.Errorf("couldn't read file %s. Err: %v", fileName, dataErr)
			}
			f, fErr := w.Create(baseInZip + file.Name())
			if fErr != nil {
				return fmt.Errorf("couldn't open zip creator. Err: %v", fErr)
			}
			_, writeErr := f.Write(data)
			if writeErr != nil {
				return fmt.Errorf("couldn't open zip writer. Err: %v", writeErr)
			}
		} else if file.IsDir() {
			newBase := path.Join(basePath, file.Name()) + "/"
			addFilesErr := addFiles(w, newBase, baseInZip+file.Name()+"/")
			if addFilesErr != nil {
				return fmt.Errorf("couldn't add files. Err: %v", addFilesErr)
			}
		}
	}
	return nil
}
