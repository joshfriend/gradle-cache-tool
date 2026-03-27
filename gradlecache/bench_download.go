package gradlecache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
)

// BenchDownloadConfig holds the parameters for an S3 download benchmark.
type BenchDownloadConfig struct {
	Bucket         string
	Region         string
	CacheKey       string
	ExtraCacheKeys []string
	Objects        []string
	GitDir         string
	Ref            string
	Commit         string
	MaxBlocks      int
	Workers        int
	ChunkMB        int
	ReadBufKB      int
	Runs           int
	Parallel       int
	Logger         *slog.Logger
}

func (c *BenchDownloadConfig) defaults() {
	if c.Region == "" {
		c.Region = defaultRegion()
	}
	if c.CacheKey == "" {
		c.CacheKey = "gradle"
	}
	if c.GitDir == "" {
		c.GitDir = "."
	}
	if c.Ref == "" {
		c.Ref = "HEAD"
	}
	if c.MaxBlocks == 0 {
		c.MaxBlocks = 20
	}
	if c.Workers == 0 {
		c.Workers = 8
	}
	if c.ChunkMB == 0 {
		c.ChunkMB = 32
	}
	if c.ReadBufKB == 0 {
		c.ReadBufKB = 1024
	}
	if c.Runs == 0 {
		c.Runs = 3
	}
	if c.Parallel == 0 {
		c.Parallel = 1
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

func (c *BenchDownloadConfig) validate() error {
	if c.Bucket == "" {
		return errors.New("Bucket is required")
	}
	if c.Workers <= 0 {
		return errors.New("Workers must be > 0")
	}
	if c.ChunkMB <= 0 {
		return errors.New("ChunkMB must be > 0")
	}
	if c.ReadBufKB <= 0 {
		return errors.New("ReadBufKB must be > 0")
	}
	if c.Runs <= 0 {
		return errors.New("Runs must be > 0")
	}
	if c.Parallel <= 0 {
		return errors.New("Parallel must be > 0")
	}
	return nil
}

// BenchDownload downloads one or more bundles to io.Discard to measure raw S3
// throughput without decompression or extraction work.
func BenchDownload(ctx context.Context, cfg BenchDownloadConfig) error {
	cfg.defaults()
	if err := cfg.validate(); err != nil {
		return err
	}

	s3, err := newS3Client(cfg.Region)
	if err != nil {
		return err
	}
	s3.dlWorkers = cfg.Workers
	s3.chunkSize = int64(cfg.ChunkMB) << 20

	numStreams := max(len(cfg.Objects), 1) * cfg.Parallel
	poolSize := cfg.Workers * numStreams
	s3.http = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        poolSize + 32,
			MaxIdleConnsPerHost: poolSize,
			WriteBufferSize:     256 << 10,
			ReadBufferSize:      256 << 10,
			DisableCompression:  true,
		},
	}

	store := &s3BundleStore{client: s3, bucket: cfg.Bucket}

	type objSpec struct {
		cacheKey string
		commit   string
		size     int64
	}

	var objects []objSpec
	if len(cfg.Objects) > 0 {
		for _, spec := range cfg.Objects {
			parts := strings.SplitN(spec, ":", 2)
			if len(parts) != 2 {
				return errors.Errorf("invalid --object %q: expected commit:cache-key", spec)
			}
			commit, key := parts[0], parts[1]
			info, statErr := store.stat(ctx, commit, key)
			if statErr != nil {
				return errors.Errorf("stat %s:%s: %w", commit[:min(8, len(commit))], key, statErr)
			}
			objects = append(objects, objSpec{cacheKey: key, commit: commit, size: info.Size})
			cfg.Logger.Info("found bundle", "key", key, "commit", commit[:min(8, len(commit))],
				"size_mb", fmt.Sprintf("%.0f", float64(info.Size)/1e6))
		}
	} else {
		allKeys := append([]string{cfg.CacheKey}, cfg.ExtraCacheKeys...)

		var commits []string
		if cfg.Commit != "" {
			commits = []string{cfg.Commit}
		} else {
			commits, err = historyCommits(ctx, cfg.GitDir, cfg.Ref, cfg.MaxBlocks)
			if err != nil {
				return errors.Wrap(err, "walk git history")
			}
		}

		for _, key := range allKeys {
			for _, sha := range commits {
				info, statErr := store.stat(ctx, sha, key)
				if statErr == nil {
					objects = append(objects, objSpec{cacheKey: key, commit: sha, size: info.Size})
					cfg.Logger.Info("found bundle", "key", key, "commit", sha[:min(8, len(sha))],
						"size_mb", fmt.Sprintf("%.0f", float64(info.Size)/1e6))
					break
				}
			}
		}
	}
	if len(objects) == 0 {
		return errors.New("no cache bundles found in history")
	}

	streamsPerObj := cfg.Parallel
	totalConnections := cfg.Workers * streamsPerObj * len(objects)
	var totalSizeMB float64
	for _, o := range objects {
		totalSizeMB += float64(o.size) / 1e6
	}

	fmt.Printf("objects: %d  total_size: %.0f MB  workers_per_stream: %d  chunk: %d MiB\n",
		len(objects), totalSizeMB*float64(streamsPerObj), cfg.Workers, cfg.ChunkMB)
	fmt.Printf("config: streams=%d×%d parallel=%d  total_connections=%d\n\n",
		len(objects), streamsPerObj, cfg.Workers, totalConnections)

	var runSpeeds []float64
	for run := 0; run < cfg.Runs; run++ {
		start := time.Now()

		var (
			mu         sync.Mutex
			wg         sync.WaitGroup
			totalBytes int64
			firstErr   error
		)

		for _, obj := range objects {
			for stream := 0; stream < streamsPerObj; stream++ {
				wg.Add(1)
				go func(o objSpec) {
					defer wg.Done()
					objKey := s3Key(o.commit, o.cacheKey, bundleFilename(o.cacheKey))
					n, getErr := discardParallelGet(ctx, s3, cfg.Bucket, objKey, o.size, cfg.ReadBufKB<<10)
					mu.Lock()
					totalBytes += n
					if getErr != nil && firstErr == nil {
						firstErr = getErr
					}
					mu.Unlock()
				}(obj)
			}
		}
		wg.Wait()
		if firstErr != nil {
			return firstErr
		}

		elapsed := time.Since(start)
		mbps := float64(totalBytes) / elapsed.Seconds() / 1e6
		gbps := mbps * 8 / 1000
		runSpeeds = append(runSpeeds, mbps)
		fmt.Printf("run %d/%d  %8s  %5.0f MB  %7.0f MB/s  %6.2f Gbps\n",
			run+1, cfg.Runs,
			elapsed.Round(time.Millisecond),
			float64(totalBytes)/1e6,
			mbps, gbps)
	}

	if len(runSpeeds) > 1 {
		var sum float64
		best := runSpeeds[0]
		for _, v := range runSpeeds {
			sum += v
			if v > best {
				best = v
			}
		}
		mean := sum / float64(len(runSpeeds))
		fmt.Printf("\nmean: %.0f MB/s (%.2f Gbps)   best: %.0f MB/s (%.2f Gbps)\n",
			mean, mean*8/1000, best, best*8/1000)
	}
	return nil
}

// discardParallelGet downloads all chunks of the object concurrently and
// streams each response body directly to io.Discard without accumulating it in
// memory. Unlike the production parallelGet, there is no in-order reassembly or
// io.Pipe round-trip, which makes this path a cleaner network benchmark.
func discardParallelGet(ctx context.Context, c *s3Client, bucket, key string, size int64, readBuf int) (int64, error) {
	numChunks := int((size + c.chunkSize - 1) / c.chunkSize)
	numWorkers := min(c.dlWorkers, numChunks)

	work := make(chan int, numChunks)
	for i := 0; i < numChunks; i++ {
		work <- i
	}
	close(work)

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		total    int64
		firstErr error
	)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, readBuf)
			for seq := range work {
				start := int64(seq) * c.chunkSize
				end := min(start+c.chunkSize-1, size-1)

				req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(bucket, key), nil)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
				c.sign(req)

				resp, err := c.http.Do(req) //nolint:gosec
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}

				n, copyErr := io.CopyBuffer(io.Discard, resp.Body, buf)
				resp.Body.Close() //nolint:errcheck,gosec

				mu.Lock()
				total += n
				if copyErr != nil && firstErr == nil {
					firstErr = copyErr
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return total, firstErr
}
