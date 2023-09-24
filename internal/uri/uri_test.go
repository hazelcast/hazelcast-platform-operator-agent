package uri

import (
	"testing"
)

func TestBasicNormalizeURI(t *testing.T) {
	tests := []struct {
		name      string
		commonURI string
		want      string
		wantErr   bool
	}{
		{"gs", "gs://bucket-name", "gs://bucket-name", false},
		{"prefix", "s3://bucket-name/hazelcast", "s3://bucket-name?prefix=hazelcast/", false},
		{"longprefix", "azblob://backup/hazelcast/2022-06-02-21-57-49/", "azblob://backup?prefix=hazelcast/2022-06-02-21-57-49/", false},
		{"query", "s3://bucket-name/hazelcast?region=us-west-1", "s3://bucket-name?prefix=hazelcast/&region=us-west-1", false},
		{"legacy", "s3://bucket-name?prefix=hazelcast/", "s3://bucket-name?prefix=hazelcast/", false},
		{"duplicate", "s3://bucket-name/hazelcast??prefix=hazelcast", "s3://bucket-name?prefix=hazelcast/", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeURI(tt.commonURI)
			if (err != nil) != tt.wantErr {
				t.Errorf("NormalizeURI() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("NormalizeURI() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAddFolderKeyToURI(t *testing.T) {
	tests := []struct {
		name      string
		commonURI string
		path      string
		want      string
		wantErr   bool
	}{
		{"with folder", "gs://bucket-name", "prefix/seq1", "gs://bucket-name?prefix=prefix/seq1", false},
		{"without prefix", "s3://bucket-name/hazelcast", "seq2", "s3://bucket-name?prefix=hazelcast/seq2", false},
		{"with prefix", "s3://bucket-name?prefix=hazelcast/", "seq2", "s3://bucket-name?prefix=hazelcast/seq2", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Join(tt.commonURI, tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("addPrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("addPrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}
