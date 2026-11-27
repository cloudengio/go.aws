// Copyright 2024 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package s3fs

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"

	"cloudeng.io/algo/container/stream"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/path/cloudpath"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	directoryBucketRE   = regexp.MustCompile(`--.*--x-s3$`)
	directoryBucketAZRE = regexp.MustCompile(`--(.*)--x-s3$`)
)

func IsDirectoryBucket(bucket string) bool {
	return directoryBucketRE.MatchString(bucket)
}

func DirectoryBucketAZ(bucket string) string {
	m := directoryBucketAZRE.FindStringSubmatch(bucket)
	if len(m) == 0 {
		return ""
	}
	return m[1]
}

var slashDelim = aws.String("/")

// NewLevelScanner returns a filewalk.LevelScanner for the specified path
// that does not prefetch.
func NewLevelScanner(client Client, delimiter byte, path string) filewalk.LevelScanner {
	return newLevelScanner(client, delimiter, path, 0)
}

func newLevelScanner(client Client, delimiter byte, path string, prefetch int) *scanner {
	path = ensureIsPrefix(path, delimiter)
	match := cloudpath.AWSS3MatcherSep(path, delimiter)
	if len(match.Matched) == 0 {
		return &scanner{err: fmt.Errorf("invalid s3 path: %v", path)}
	}
	sc := &scanner{
		pager: pager{
			client:    client,
			bucket:    aws.String(match.Volume),
			prefix:    aws.String(match.Key),
			delim:     aws.String(string(delimiter)),
			delimByte: delimiter,
		},
		match:    match,
		prefetch: prefetch,
	}
	if IsDirectoryBucket(match.Volume) {
		sc.delim = slashDelim
	}
	return sc
}

func (fs *T) LevelScanner(prefix string) filewalk.LevelScanner {
	return newLevelScanner(fs.client, fs.options.delimiter, prefix, fs.options.prefetch)
}

// pager lists the pages of a single prefix.
type pager struct {
	client         Client
	bucket, prefix *string
	delim          *string
	delimByte      byte
	token          *string
	done           bool
}

type scanner struct {
	pager
	match    cloudpath.Match
	entries  []filewalk.Entry
	err      error
	closed   bool
	prefetch int // see WithPrefetch.
	read     int // entries read so far.
	pf       *stream.Scanner[filewalk.Entry]
}

func (sc *scanner) Contents() []filewalk.Entry {
	return sc.entries
}

func (sc *scanner) Err() error {
	return sc.err
}

// Close stops any prefetching.
func (sc *scanner) Close() error {
	sc.closed = true
	if sc.pf != nil {
		_ = sc.pf.Close()
		sc.pf = nil
	}
	return nil
}

func (sc *scanner) Scan(ctx context.Context, n int) bool {
	if sc.err != nil || sc.done || sc.closed {
		return false
	}
	if sc.pf != nil {
		return sc.scanPrefetched(n)
	}
	entries, err := sc.list(ctx, n)
	if err != nil {
		sc.err = err
		return false
	}
	sc.entries = entries
	sc.read += len(entries)
	if sc.prefetch > 0 && sc.read >= sc.prefetch && !sc.done {
		sc.startPrefetch(ctx, n)
	}
	return len(entries) != 0
}

// list returns the next non-empty page of up to n entries, or no entries
// once the listing is complete, which is also recorded in p.done.
func (p *pager) list(ctx context.Context, n int) ([]filewalk.Entry, error) {
	for {
		req := s3.ListObjectsV2Input{
			Bucket:            p.bucket,
			Prefix:            p.prefix,
			ContinuationToken: p.token,
			Delimiter:         p.delim,
			MaxKeys:           aws.Int32(int32(n)), //nolint:gosec // n is a page size.
		}
		obj, err := p.client.ListObjectsV2(ctx, &req)
		if err != nil {
			return nil, err
		}
		p.done = !aws.ToBool(obj.IsTruncated)
		p.token = obj.NextContinuationToken
		if entries := convertListObjectsOutput(obj, p.delimByte); len(entries) > 0 || p.done {
			return entries, nil
		}
	}
}

// startPrefetch lists the remaining pages in the background using
// a copy of the pager, which the goroutine that does so then owns.
func (sc *scanner) startPrefetch(ctx context.Context, n int) {
	p := sc.pager
	popn := func(ctx context.Context, items []filewalk.Entry) (int, bool, error) {
		entries, err := p.list(ctx, len(items))
		return copy(items, entries), p.done, err
	}
	sc.pf = stream.NewScanner(ctx, popn, stream.WithBatchSize(n))
}

func (sc *scanner) scanPrefetched(n int) bool {
	sc.entries = nil
	for len(sc.entries) < n && sc.pf.Scan() {
		sc.entries = append(sc.entries, sc.pf.Data())
	}
	if len(sc.entries) > 0 {
		return true
	}
	sc.err = sc.pf.Err()
	_ = sc.Close()
	return false
}

func convertListObjectsOutput(lo *s3.ListObjectsV2Output, delim byte) []filewalk.Entry {
	ne := len(lo.Contents) + len(lo.CommonPrefixes)
	if ne == 0 {
		return nil
	}
	entries := make([]filewalk.Entry, ne)
	for i, c := range lo.Contents {
		entries[i].Name = cloudpath.Base("s3://", delim, aws.ToString(c.Key))
		entries[i].Type = fs.FileMode(0)
	}
	n := len(lo.Contents)
	for i, p := range lo.CommonPrefixes {
		// Need the name of the parent prefix as a prefix, eg.
		// for s3://a/b/ want b/.
		pref := aws.ToString(p.Prefix)
		pref = pref[:len(pref)-1]
		entries[n+i].Name = cloudpath.Base("s3://", delim, pref) + string(delim)
		entries[n+i].Type = fs.ModeDir
	}
	return entries
}
