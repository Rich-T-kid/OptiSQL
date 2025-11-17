package project

import (
	"io"
	"os"
	"strings"
	"testing"
)

const (
	s3CSVFile     = "country_full.csv"
	s3ParquetFile = "userdata.parquet"
	s3TxtFile     = "example.txt"
)

// test s3 as a source first then run test for other source files here
func TestS3(t *testing.T) {
	// Simple passing test
	_, err := NewStreamReader(s3CSVFile)
	if err != nil {
		t.Fatalf("failed to create s3 stream reader: %v", err)
	}
}

// test for
// (1) reading files from network (s3) should provide exact same abstraction as a local file
func TestS3BasicRead(t *testing.T) {
	t.Run("csv read", func(t *testing.T) {
		nr, err := NewStreamReader(s3CSVFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		firstKB := make([]byte, 1024)
		n, err := nr.stream.Read(firstKB)
		if err != nil {
			t.Fatalf("failed to read from s3 object: %v", err)
		}
		if n != 1024 {
			t.Fatalf("expected to read 1024 bytes, but read %d bytes", n)
		}
		t.Logf("returned contents %s\n", firstKB[:n])

	})
	t.Run("parquet read", func(t *testing.T) {
		nr, err := NewStreamReader(s3ParquetFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		firstKB := make([]byte, 1024)
		n, err := nr.stream.Read(firstKB)
		if err != nil {
			t.Fatalf("failed to read from s3 object: %v", err)
		}
		if n != 1024 {
			t.Fatalf("expected to read 1024 bytes, but read %d bytes", n)
		}
		t.Logf("returned contents %v\n", firstKB[:n])

	})
	t.Run("txt read", func(t *testing.T) {
		nr, err := NewStreamReader(s3TxtFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		firstKB := make([]byte, 1024)
		n, err := nr.stream.Read(firstKB)
		if err != nil {
			t.Fatalf("failed to read from s3 object: %v", err)
		}
		if n != 1024 {
			t.Fatalf("expected to read 1024 bytes, but read %d bytes", n)
		}
		t.Logf("returned contents %s\n", firstKB[:n])

	})
}

// (2) download entire file before reading
func TestS3Download(t *testing.T) {
	t.Run("Download CSV locally", func(t *testing.T) {
		nr, err := NewStreamReader(s3CSVFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		newFile, err := nr.DownloadLocally()
		if err != nil {
			t.Fatalf("failed to download file locally %v", err)
		}
		defer func() {
			_ = newFile.Close()
			if err := os.Remove(newFile.Name()); err != nil {
				t.Fatalf("error closing file %v", newFile.Name())
			}
		}()
		// validate stats about file
		info, err := newFile.Stat()
		if err != nil {
			t.Fatalf("failed to get file stats %v", err)
		}
		if info.IsDir() {
			t.Fatalf("expected regular file, found directory: %s", info.Name())
		}

		if info.Size() < 100 {
			t.Fatalf("file is too small (%d bytes), expected >= 100 bytes", info.Size())
		}

		if !strings.HasPrefix(info.Name(), nr.fileName) {
			t.Fatalf("filename mismatch: got %s, expected prefix %s", info.Name(), nr.fileName)
		}

	})
	t.Run("Download parquet locally", func(t *testing.T) {
		nr, err := NewStreamReader(s3ParquetFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		newFile, err := nr.DownloadLocally()
		if err != nil {
			t.Fatalf("failed to download file locally %v", err)
		}
		defer func() {
			_ = newFile.Close()
			if err := os.Remove(newFile.Name()); err != nil {
				t.Fatalf("error closing file %v", newFile.Name())
			}
		}()
		// validate stats about file
		info, err := newFile.Stat()
		if err != nil {
			t.Fatalf("failed to get file stats %v", err)
		}
		if info.IsDir() {
			t.Fatalf("expected regular file, found directory: %s", info.Name())
		}

		if info.Size() < 100 {
			t.Fatalf("file is too small (%d bytes), expected >= 100 bytes", info.Size())
		}

		if !strings.HasPrefix(info.Name(), nr.fileName) {
			t.Fatalf("filename mismatch: got %s, expected prefix %s", info.Name(), nr.fileName)
		}

	})
	t.Run("Download txt locally", func(t *testing.T) {
		nr, err := NewStreamReader(s3TxtFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		newFile, err := nr.DownloadLocally()
		if err != nil {
			t.Fatalf("failed to download file locally %v", err)
		}
		defer func() {
			_ = newFile.Close()
			if err := os.Remove(newFile.Name()); err != nil {
				t.Fatalf("error closing file %v", newFile.Name())
			}
		}()
		// validate stats about file
		info, err := newFile.Stat()
		if err != nil {
			t.Fatalf("failed to get file stats %v", err)
		}
		if info.IsDir() {
			t.Fatalf("expected regular file, found directory: %s", info.Name())
		}

		if info.Size() < 100 {
			t.Fatalf("file is too small (%d bytes), expected >= 100 bytes", info.Size())
		}

		if !strings.HasPrefix(info.Name(), nr.fileName) {
			t.Fatalf("filename mismatch: got %s, expected prefix %s", info.Name(), nr.fileName)
		}

	})
}

// (3) add s3 variant of existing operaor sources (csv,parquet) and write minimal test here to check they work the same
func TestS3ForSource(t *testing.T) {
	t.Run("csv from s3 source", func(t *testing.T) {
		nr, err := NewStreamReader(s3CSVFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		pj, err := NewProjectCSVLeaf(nr.stream)
		if err != nil {
			t.Fatalf("failed to create csv project source from s3 object: %v", err)
		}
		rc, err := pj.Next(5)
		if err != nil {
			t.Fatalf("failed to read record batch from s3 csv source: %v", err)
		}
		t.Logf("returned record batch from s3 csv source: %v\n", rc)

	})
	t.Run("parquet from s3 source", func(t *testing.T) {
		nr, err := NewStreamReader(s3ParquetFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		pj, err := NewParquetSource(nr)
		if err != nil {
			t.Fatalf("failed to create parquet project source from s3 object: %v", err)
		}
		rc, err := pj.Next(5)
		if err != nil {
			t.Fatalf("failed to read record batch from s3 csv source: %v", err)
		}
		t.Logf("returned record batch from s3 csv source: %v\n", rc)

	})
	t.Run("csv from s3 source then downloaded", func(t *testing.T) {
		nr, err := NewStreamReader(s3CSVFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		f, err := nr.DownloadLocally()
		if err != nil {
			t.Fatalf("failed to download s3 object locally: %v", err)
		}
		defer func() {
			t.Log("deleting downloaded file...")
			_ = f.Close()
			if err := os.Remove(f.Name()); err != nil {
				t.Fatalf("error closing file %v", f.Name())
			}
		}()
		pj, err := NewProjectCSVLeaf(f)
		if err != nil {
			t.Fatalf("failed to create csv project source from s3 object: %v", err)
		}
		rc, err := pj.Next(5)
		if err != nil {
			t.Fatalf("failed to read record batch from s3 csv source: %v", err)
		}
		err = pj.Close()
		if err != nil {
			t.Fatalf("failed to close csv project source: %v", err)
		}
		t.Logf("returned record batch from s3 csv source: %v\n", rc)

	})
	t.Run("parquet from s3 source then downloaded", func(t *testing.T) {
		nr, err := NewStreamReader(s3ParquetFile)
		if err != nil {
			t.Fatalf("failed to create s3 object: %v", err)
		}
		f, err := nr.DownloadLocally()
		if err != nil {
			t.Fatalf("failed to download s3 object locally: %v", err)
		}
		defer func() {
			t.Log("deleting downloaded file...")
			_ = f.Close()
			if err := os.Remove(f.Name()); err != nil {
				t.Fatalf("error closing file %v", f.Name())
			}
		}()
		pj, err := NewParquetSource(f)
		if err != nil {
			t.Fatalf("failed to create csv project source from s3 object: %v", err)
		}
		rc, err := pj.Next(5)
		if err != nil {
			t.Fatalf("failed to read record batch from s3 csv source: %v", err)
		}
		t.Logf("returned record batch from s3 csv source: %v\n", rc)

	})
}

func TestS3Source(t *testing.T) {
	nr, err := NewStreamReader(s3CSVFile)
	if err != nil {
		t.Fatalf("failed to create s3 object: %v", err)
	}
	t.Run("test SeekStart", func(t *testing.T) {
		_, err := nr.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatalf("failed to seek to start of s3 object: %v", err)
		}
	})
	t.Run("invalidSeek ", func(t *testing.T) {
		_, err := nr.Seek(4, 4)
		if err == nil {
			t.Fatalf("expected error when seeking with invalid whence, but got none")
		}
	})
	t.Run("test stream read", func(t *testing.T) {
		stream := nr.Stream()
		buf := make([]byte, 512)
		n, err := stream.Read(buf)
		if err != nil {
			t.Fatalf("failed to read from s3 object stream: %v", err)
		}
		if n == 0 {
			t.Fatalf("expected to read some bytes from s3 object stream, but read 0 bytes")
		}
		t.Logf("read %d bytes from s3 object stream: %s\n", n, string(buf[:n]))
	})
}
