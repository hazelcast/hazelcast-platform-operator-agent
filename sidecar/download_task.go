package sidecar

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"gocloud.dev/blob"

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
	return fileutil.DownloadFileFromURL(ctx, req.URL, req.DestDir)
}

func downloadBundle(ctx context.Context, req BundleReq) error {
	secretData, err := bucket.SecretData(ctx, req.SecretName)
	if err != nil {
		return err
	}
	bucketURI, err := uri.NormalizeURI(req.URL)
	if err != nil {
		return err
	}

	f, err := os.Create(filepath.Clean(req.DestDir))
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := bucket.OpenBucket(ctx, bucketURI, secretData)
	if err != nil {
		return err
	}
	defer b.Close()

	w := zip.NewWriter(f)
	iter := b.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// we only want top level files and no files under sub-folders
		if path.Base(obj.Key) != obj.Key {
			continue
		}
		err = readZip(ctx, b, obj, w)
		if err != nil {
			return err
		}
	}
	return w.Close()
}

func readZip(ctx context.Context, b *blob.Bucket, obj *blob.ListObject, w *zip.Writer) error {
	r, err := b.NewReader(ctx, obj.Key, nil)
	if err != nil {
		return err
	}
	defer r.Close()
	f, err := w.Create(obj.Key)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	if err != nil {
		return err
	}
	return nil
}
