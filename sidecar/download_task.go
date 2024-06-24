package sidecar

import (
	"context"
	"fmt"

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
	bucketURI, err := uri.NormalizeURI(req.URL)
	if err != nil {
		return fmt.Errorf("error occurred while parsing bucket URI: %w", err)
	}
	err = bucket.DownloadFile(ctx, bucketURI, req.DestDir, req.FileName, req.SecretName)
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	return nil
}

func downloadFromUrl(ctx context.Context, req DownloadFileReq) error {
	return fileutil.DownloadFileFromURL(ctx, req.URL, req.DestDir)
}
