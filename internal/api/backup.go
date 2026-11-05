package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	BackupPath        = "/v1/admin/backup"
	BackupContentType = "application/octet-stream"
	maxBackupResponse = (8 << 30) + 1024
	backupHeaderWait  = 90 * time.Second
)

type backupStream struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b backupStream) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}

// OpenBackup locates a ready coordinator and returns its archive stream.
// A partial stream is never retried into the same output.
func (c *Client) OpenBackup(ctx context.Context) (io.ReadCloser, int64, error) {
	client := &http.Client{Transport: c.http.Transport, CheckRedirect: c.http.CheckRedirect}
	retryUntil := time.Now().Add(clientTimeout)
	var last error
	for delay := clusterRetryDelay; ctx.Err() == nil; delay = min(delay*2, maxClusterBackoff) {
		start := int(c.preferred.Load())
		for offset := range c.origins {
			if ctx.Err() != nil {
				break
			}
			index := (start + offset) % len(c.origins)
			body, size, status, err := c.tryBackup(ctx, client, c.origins[index])
			if err == nil {
				c.preferred.Store(uint32(index))
				return body, size, nil
			}
			if !retryable(status) {
				return nil, 0, err
			}
			if status != 0 || StatusCode(last) == 0 {
				last = err
			}
		}
		if len(c.origins) == 1 || !time.Now().Before(retryUntil) {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
	}
	return nil, 0, errors.Join(last, ctx.Err())
}

func (c *Client) tryBackup(ctx context.Context, client *http.Client, origin string) (io.ReadCloser, int64, int, error) {
	attempt, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(attempt, http.MethodGet, origin+BackupPath, nil)
	if err != nil {
		cancel()
		return nil, 0, 0, err
	}
	req.Header.Set("Accept", BackupContentType)
	req.Header.Set("Accept-Encoding", "identity")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	// Bound the wait for an unresponsive origin, while the caller's context
	// controls the duration of a successful download.
	timer := time.AfterFunc(backupHeaderWait, cancel)
	res, err := client.Do(req)
	timer.Stop()
	if err != nil {
		cancel()
		return nil, 0, 0, err
	}
	stream := backupStream{ReadCloser: res.Body, cancel: cancel}
	if res.StatusCode != http.StatusOK {
		defer stream.Close()
		return nil, 0, res.StatusCode, readResponseError(res)
	}
	if res.ContentLength <= 0 || res.ContentLength > maxBackupResponse ||
		res.Header.Get("Content-Type") != BackupContentType {
		stream.Close()
		return nil, 0, res.StatusCode, fmt.Errorf("invalid coordinator backup response (size %d)", res.ContentLength)
	}
	return stream, res.ContentLength, res.StatusCode, nil
}
