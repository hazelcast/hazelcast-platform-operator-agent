package main

import (
	"context"
	"testing"

	"gocloud.dev/blob/memblob"
)

func TestFind(t *testing.T) {
	tests := []struct {
		name    string
		keys    []string
		id      int
		want    string
		wantErr bool
	}{
		{"extension", []string{"foo"}, 0, "", false},
		{"single", []string{"foo.tar.gz"}, 0, "foo.tar.gz", false},
		{"id", []string{"a.tar.gz", "b.tar.gz"}, 1, "b.tar.gz", false},
		{"overflow", []string{"foo.tar.gz"}, 99, "", false},
		{
			"single with date",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
			},
			0,
			"2006-01-02-15-04-01/a.tar.gz",
			false,
		},
		{
			"id with date",
			[]string{
				"2006-01-02-15-04-01/a.tar.gz",
				"2006-01-02-15-04-01/b.tar.gz",
			},
			1,
			"2006-01-02-15-04-01/b.tar.gz",
			false,
		},
		{
			"latest",
			[]string{
				"2006-01-02-15-04-01/foo.tar.gz",
				"2006-01-02-15-04-02/foo.tar.gz",
				"2022-06-13-00-00-00/foo.tar.gz",
			},
			0,
			"2022-06-13-00-00-00/foo.tar.gz",
			false,
		},
		{
			"mixed",
			[]string{
				"bar.tar.gz",
				"foo/bar.tar.gz",
				"2006-01-02-15-04-01/foo.tar.gz",
			},
			0,
			"2006-01-02-15-04-01/foo.tar.gz",
			false,
		},
		// {
		// 	"top",
		// 	[]string{
		// 		"foo/2006-01-02-15-04-02/foo.tar.gz",
		// 		"bar/2006-01-02-15-04-01/foo.tar.gz",
		// 	},
		// 	0,
		// 	"",
		// 	true,
		// },
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			bucket := memblob.OpenBucket(nil)
			defer bucket.Close()
			for _, k := range tt.keys {
				if err := bucket.WriteAll(ctx, k, []byte(""), nil); err != nil {
					t.Fatal(err)
				}
			}

			// test
			got, err := find(ctx, bucket, tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("find() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("find() = %v, want %v", got, tt.want)
			}
		})
	}
}
