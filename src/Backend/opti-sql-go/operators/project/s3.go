package project

import (
	"fmt"
	"io"
	"opti-sql-go/config"
	"os"
	"time"

	"github.com/minio/minio-go"
)

var secretes = config.GetConfig().Secretes

type mime string

var (
	MimeCSV     mime = "csv"
	MimeParquet mime = "parquet"
)

type NetworkResource struct {
	client *minio.Client
	bucket string
	key    string

	// raw streaming object for CSV
	stream *minio.Object
	// for clean up-testing
	fileName string
}

func NewStreamReader(fileName string) (*NetworkResource, error) {
	accessKey := secretes.AccessKey
	secretKey := secretes.SecretKey
	endpoint := secretes.EndpointURL
	bucket := secretes.BucketName
	useSSL := true

	client, err := minio.New(endpoint, accessKey, secretKey, useSSL)
	if err != nil {
		return nil, err
	}

	obj, err := client.GetObject(bucket, fileName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return &NetworkResource{
		client:   client,
		bucket:   bucket,
		key:      fileName,
		fileName: fileName,
		stream:   obj, // CSV reads this directly
	}, nil
}

func (n *NetworkResource) Stream() io.Reader {
	return n.stream
}

// S3ReaderAt implements io.ReaderAt for Parquet readers
func (n *NetworkResource) ReadAt(p []byte, off int64) (int, error) {
	opts := minio.GetObjectOptions{}
	_ = opts.SetRange(off, off+int64(len(p))-1)

	obj, err := n.client.GetObject(n.bucket, n.key, opts)
	if err != nil {
		return 0, err
	}
	return io.ReadFull(obj, p)
}

func (n *NetworkResource) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return offset, nil
	case io.SeekEnd:
		// Need to return total object size
		info, err := n.client.StatObject(n.bucket, n.key, minio.StatObjectOptions{})
		if err != nil {
			return 0, fmt.Errorf("failed to stat object: %w", err)
		}
		return info.Size, nil
	default:
		return 0, fmt.Errorf("unsupported seek mode for S3: %d", whence)
	}
}
func (n *NetworkResource) DownloadLocally() (*os.File, error) {
	f, err := os.Create(fmt.Sprintf("%s-%d", n.key, time.Now().UnixNano()))
	if err != nil {
		return nil, err
	}

	// Read entire stream once
	content, err := io.ReadAll(n.stream)
	if err != nil {
		return nil, err
	}

	if _, err := f.Write(content); err != nil {
		return nil, err
	}

	// Rewind so CSV readers can start from beginning
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return f, nil
}
