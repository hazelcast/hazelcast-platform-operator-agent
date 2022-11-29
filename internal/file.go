package internal

import (
	"os"
	"path"
	"path/filepath"
	"strings"
)

type File struct {
	Name  string
	IsDir bool
}

func CreateFiles(pth string, files []File, createDir bool) error {
	if createDir {
		err := os.MkdirAll(pth, 0700)
		if err != nil {
			return err
		}
	}
	for _, file := range files {
		if file.IsDir {
			err := os.MkdirAll(path.Join(pth, file.Name), 0700)
			if err != nil {
				return err
			}
			continue
		}

		err := os.MkdirAll(path.Join(pth, path.Dir(file.Name)), 0700)
		if err != nil {
			return err
		}

		_, err = os.Create(path.Join(pth, file.Name))
		if err != nil {
			return err
		}
	}
	return nil
}

func DirFileList(baseDir string) ([]File, error) {
	var files []File
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if baseDir == path {
			return nil
		}
		fileName := strings.TrimPrefix(path, baseDir+"/")
		files = append(files, File{Name: fileName, IsDir: info.IsDir()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
