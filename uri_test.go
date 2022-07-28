package main

import (
	"testing"
)

func TestBasicFormatURI(t *testing.T) {
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
			got, err := formatURI(tt.commonURI)
			if (err != nil) != tt.wantErr {
				t.Errorf("formatURI() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("formatURI() = %v, want %v", got, tt.want)
			}
		})
	}
}
