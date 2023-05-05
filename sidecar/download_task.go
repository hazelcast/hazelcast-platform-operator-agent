package sidecar

import (
	"context"
	"fmt"
	"strings"

	"github.com/hazelcast/platform-operator-agent/internal/bucket"
	"github.com/hazelcast/platform-operator-agent/internal/fileutil"
	"github.com/hazelcast/platform-operator-agent/internal/uri"
)

func downloadFile(ctx context.Context, req DownloadFileReq) error {
	if req.DownloadType == URLDownload {
		return downloadFromUrl(ctx, req)
	}
	return downloadFromBucket(ctx, req)
}

func downloadFromBucket(ctx context.Context, req DownloadFileReq) error {
	data, err := bucket.SecretData(ctx, req.SecretName)
	if err != nil {
		return fmt.Errorf("error fetching secret data: %w", err)
	}
	bucketURI, err := uri.NormalizeURI(req.URL)
	if err != nil {
		return fmt.Errorf("error occurred while parsing bucket URI: %w", err)
	}
	err = bucket.DownloadFile(ctx, bucketURI, req.DestDir, req.FileName, data)
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	return nil
}

func downloadFromUrl(ctx context.Context, req DownloadFileReq) error {
	srcURL := req.URL
	if !strings.HasSuffix(req.URL, ".jar") {
		srcURL = strings.TrimSuffix(req.URL, "/") + "/" + req.FileName
	}
	return fileutil.DownloadFileFromURL(ctx, srcURL, req.DestDir)
}
