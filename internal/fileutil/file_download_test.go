package fileutil

import (
	"context"
	"net/http"
	"os"
	"path"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

type httpContent struct {
	contentType         string
	contentDispFileName string
}

func TestDownloadFileFromURL(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	tests := []struct {
		name      string
		url       string
		content   httpContent
		wantFiles []File
		wantErr   error
	}{
		{
			"using path without extension",
			"http://example.com/without_extension",
			httpContent{},
			[]File{{Name: "without_extension"}},
			nil,
		},
		{
			"without any filename info",
			"http://example.com",
			httpContent{},
			[]File{},
			ErrNoFilename,
		},
		{
			"using path with extension",
			"http://example.com/with_extension.jar",
			httpContent{},
			[]File{{Name: "with_extension.jar"}},
			nil,
		},
		{
			"filename from content-disposition filename",
			"http://example.com/from_path.jar",
			httpContent{contentDispFileName: "from_content_disposition.jar"},
			[]File{{Name: "from_content_disposition.jar"}},
			nil,
		},
		{
			"filename from content-disposition filename and extension from content-type",
			"http://example.com",
			httpContent{contentType: "application/json", contentDispFileName: "from_content_disposition"},
			[]File{{Name: "from_content_disposition.json"}},
			nil,
		},
		{
			"filename and extension from only content-disposition",
			"http://example.com",
			httpContent{contentType: "application/json", contentDispFileName: "content.extension"},
			[]File{{Name: "content.extension"}},
			nil,
		},
		{
			"filename from path and extension from content-type",
			"http://example.com/from_path",
			httpContent{contentType: "application/java-archive"},
			[]File{{Name: "from_path.jar"}},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer httpmock.Reset()

			// Prepare the buckets and destination folder
			tmpdir, err := os.MkdirTemp("", "tmpDir")
			require.Nil(t, err)

			//defer os.RemoveAll(tmpdir)
			dstPath, err := os.MkdirTemp(tmpdir, "dest")
			require.Nil(t, err, "Destination Path could not be created")

			httpmock.RegisterResponder("GET", tt.url,
				AnyResponse(200, nil, tt.content.contentType, tt.content.contentDispFileName))

			// Run the tests
			err = DownloadFileFromURL(context.Background(), tt.url, dstPath)
			require.Equal(t, tt.wantErr, err, "Error is: ", err)
			if err != nil {
				return
			}
			copiedFiles, err := DirFileList(dstPath)
			require.Nil(t, err)
			require.ElementsMatch(t, copiedFiles, tt.wantFiles)
		})
	}
}

func TestDownloadFileFromURL_DestinationPath(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	tests := []struct {
		name          string
		dstPathExists bool
		url           string
		content       httpContent
		wantFiles     []File
		wantErr       bool
	}{
		{
			"without existing destination path",
			false,
			"http://example.com/filename",
			httpContent{},
			[]File{},
			true,
		},
		{
			"with existing destination path",
			true,
			"http://example.com/filename",
			httpContent{},
			[]File{{Name: "filename"}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer httpmock.Reset()

			// Prepare the buckets and destination folder
			tmpdir, err := os.MkdirTemp("", "tmpDir")
			require.Nil(t, err)
			defer os.RemoveAll(tmpdir)

			var dstPath string
			if tt.dstPathExists {
				dstPath, err = os.MkdirTemp(tmpdir, "dest")
				require.Nil(t, err, "Destination Path could not be created")
			} else {
				dstPath = path.Join(tmpdir, "dest-does-not-exist")
			}

			httpmock.RegisterResponder("GET", tt.url,
				AnyResponse(200, nil, tt.content.contentType, tt.content.contentDispFileName))

			// Run the tests
			err = DownloadFileFromURL(context.Background(), tt.url, dstPath)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				require.ErrorContains(t, err, "no such file or directory")
				return
			}
			copiedFiles, err := DirFileList(dstPath)
			require.Nil(t, err)
			require.ElementsMatch(t, copiedFiles, tt.wantFiles)
		})
	}
}

func AnyResponse(status int, bytes []byte, conType, conFileName string) func(req *http.Request) (*http.Response, error) {
	return func(req *http.Request) (*http.Response, error) {
		response := httpmock.NewBytesResponse(status, bytes)

		if conType != "" {
			response.Header.Set("Content-Type", conType)
		}

		if conFileName != "" {
			response.Header.Set("Content-Disposition", "attachment; filename="+conFileName)
		}
		response.Request = req
		return response, nil
	}
}
