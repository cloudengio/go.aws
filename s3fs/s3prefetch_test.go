// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package s3fs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	"cloudeng.io/aws/s3fs"
	"cloudeng.io/file/filewalk"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// listClient serves ListObjectsV2 for nkeys keys, paged by MaxKeys.
type listClient struct {
	s3fs.Client
	nkeys   int
	failAt  int // fail the request with this continuation offset.
	maxKeys []int
}

func (c *listClient) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	start := 0
	if in.ContinuationToken != nil {
		start, _ = strconv.Atoi(*in.ContinuationToken)
	}
	if c.failAt > 0 && start == c.failAt {
		return nil, errors.New("list failed")
	}
	n := int(aws.ToInt32(in.MaxKeys))
	c.maxKeys = append(c.maxKeys, n)
	end := min(start+n, c.nkeys)
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(end < c.nkeys)}
	for i := start; i < end; i++ {
		out.Contents = append(out.Contents, types.Object{Key: aws.String(fmt.Sprintf("dir/k%06d", i))})
	}
	if end < c.nkeys {
		out.NextContinuationToken = aws.String(strconv.Itoa(end))
	}
	return out, nil
}

func scanAll(ctx context.Context, sc filewalk.LevelScanner, n int) (int, error) {
	total := 0
	for sc.Scan(ctx, n) {
		total += len(sc.Contents())
	}
	return total, sc.Err()
}

func TestScannerPrefetch(t *testing.T) {
	ctx := context.Background()
	for _, nkeys := range []int{0, 1, 99, 100, 101, 1050} {
		for _, prefetch := range []int{0, 1, 100, 500, 5000} {
			for _, n := range []int{1, 10, 100, 300} {
				client := &listClient{nkeys: nkeys}
				fs := s3fs.NewS3FS(aws.Config{}, s3fs.WithS3Client(client), s3fs.WithPrefetch(prefetch))
				got, err := scanAll(ctx, fs.LevelScanner("s3://bucket/dir/"), n)
				if err != nil || got != nkeys {
					t.Errorf("keys %v prefetch %v n %v: got %v, %v", nkeys, prefetch, n, got, err)
				}
			}
		}
	}
}

func TestScannerPrefetchError(t *testing.T) {
	client := &listClient{nkeys: 1000, failAt: 300}
	fs := s3fs.NewS3FS(aws.Config{}, s3fs.WithS3Client(client), s3fs.WithPrefetch(1))
	got, err := scanAll(context.Background(), fs.LevelScanner("s3://bucket/dir/"), 100)
	if err == nil || err.Error() != "list failed" {
		t.Errorf("got %v, want list failed", err)
	}
	if got != 300 {
		t.Errorf("got %v entries, want 300", got)
	}
}

func TestScannerPrefetchClose(t *testing.T) {
	client := &listClient{nkeys: 1000}
	fs := s3fs.NewS3FS(aws.Config{}, s3fs.WithS3Client(client), s3fs.WithPrefetch(1))
	sc := fs.LevelScanner("s3://bucket/dir/")
	for range 2 {
		if !sc.Scan(context.Background(), 100) {
			t.Fatalf("scan failed: %v", sc.Err())
		}
	}
	if err := sc.(io.Closer).Close(); err != nil {
		t.Fatal(err)
	}
	if sc.Scan(context.Background(), 100) {
		t.Error("expected Scan to return false after Close")
	}
}
