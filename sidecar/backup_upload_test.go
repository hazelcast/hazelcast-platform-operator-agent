package sidecar

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertHumanReadableFormat(t *testing.T) {
	tests := []struct {
		name    string
		date    string
		want    string
		wantErr bool
	}{
		{
			"Only UTC/GMT time zone should work", "backup-1659457880416", "2022-08-02-16-31-20", false,
		},
		{
			"incorrect input should fail", "backup-1659aaaa", "", true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i, err := convertHumanReadableFormat(tt.date)
			require.Equal(t, tt.wantErr, err != nil, "Error is: ", err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.want, i)
		})
	}
}
