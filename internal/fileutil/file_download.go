package fileutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var (
	ErrNoFilename = errors.New("no file exists")
)

func init() {
	// .jar extension is not present by default
	err := mime.AddExtensionType(".jar", "application/java-archive")
	if err != nil {
		panic(err)
	}
}

func DownloadFileFromURL(ctx context.Context, srcURL, dstFolder string) error {
	// Get the data
	resp, err := http.Get(srcURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if c := resp.StatusCode; c < 200 || 299 < c {
		return fmt.Errorf("error downloading file, status code is %d", c)
	}

	// Guess the filename
	fileName := guessFilename(resp)
	if fileName == "" {
		return ErrNoFilename
	}

	// Create the file
	out, err := os.Create(path.Join(dstFolder, fileName))
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	// flush file
	if err = out.Sync(); err != nil {
		return err
	}

	return nil
}

// Code snippet taken from https://github.com/cavaliergopher/grab/blob/v3.0.1/v3/util.go
func guessFilename(resp *http.Response) string {
	filename := resp.Request.URL.Path
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if val, ok := params["filename"]; ok {
				filename = val
			} // else filename directive is missing.. fallback to URL.Path
		}
	}

	// sanitize
	if filename == "" || strings.HasSuffix(filename, "/") || strings.Contains(filename, "\x00") {
		return ""
	}

	filename = filepath.Base(path.Clean("/" + filename))
	if filename == "" || filename == "." || filename == "/" {
		return ""
	}

	// add extension if needed
	if !strings.Contains(filename, ".") {
		ext := getExtensionFromHeader(resp)
		filename += ext
	}

	return filename
}

func getExtensionFromHeader(resp *http.Response) string {
	ct := resp.Header.Get("content-type")
	if ct == "" {
		return ""
	}

	mediatype, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return ""
	}

	typ, err := mime.ExtensionsByType(mediatype)
	if err != nil {
		return ""
	}

	if len(typ) == 0 {
		return ""
	}
	return typ[0]
}
