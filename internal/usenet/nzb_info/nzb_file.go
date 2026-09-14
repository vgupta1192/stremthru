package nzb_info

import (
	"bytes"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/MunifTanjim/stremthru/internal/cache"
	"github.com/MunifTanjim/stremthru/internal/config"
	"github.com/MunifTanjim/stremthru/internal/logger"
	newznab_indexer "github.com/MunifTanjim/stremthru/internal/newznab/indexer"
	"github.com/MunifTanjim/stremthru/internal/util"
	"golang.org/x/sync/singleflight"
)

type NZBFile struct {
	Blob []byte
	Name string
	Link string
	Mod  time.Time
}

func (b NZBFile) Size() int64 {
	return int64(len(b.Blob))
}

func (b NZBFile) CacheSize() int64 {
	return b.Size()
}

func (f *NZBFile) ToFileHeader() (*multipart.FileHeader, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("file", f.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err := part.Write(f.Blob); err != nil {
		return nil, fmt.Errorf("failed to write file data: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	reader := multipart.NewReader(&buf, writer.Boundary())
	form, err := reader.ReadForm(f.Size() + 1024)
	if err != nil {
		return nil, fmt.Errorf("failed to read form: %w", err)
	}

	files, ok := form.File["file"]
	if !ok || len(files) == 0 {
		return nil, fmt.Errorf("failed to extract file header")
	}

	return files[0], nil
}

var nzbFileCache = cache.NewCache[NZBFile](&cache.CacheConfig{
	Name:       "newz_nzb",
	Lifetime:   config.Newz.NZBFileCacheTTL,
	DiskBacked: true,
	MaxSize:    config.Newz.NZBFileCacheSize,
})

func IsNZBFileCached(hash string) bool {
	return nzbFileCache.Has(hash)
}

func GetCachedNZBFile(hash string) *NZBFile {
	var nzbFile NZBFile
	if nzbFileCache.Get(hash, &nzbFile) {
		return &nzbFile
	}
	return nil
}

var nzbFetchErrCache = cache.NewCache[string](&cache.CacheConfig{
	Name:     "newz_nzb_fetch_failure",
	Lifetime: 5 * time.Minute,
})

func RehashIfNeeded(info *NZBInfo) error {
	newHash := util.HashNZBFileLink(info.URL)
	if info.Hash == newHash {
		return nil
	}
	return UpdateHash(info.Id, newHash)
}

var nzbFileFetchSG singleflight.Group

// Confirmed live (2026-09-13): fetching the raw .nzb XML for a very large
// release (a 2160p REMUX's NZB alone was 14.2MB - tens of thousands of
// segment references) took 15.8s on a clean try, and genuinely exceeded
// 60s at least once under this host's typical heavy load, producing
// "context deadline exceeded (Client.Timeout exceeded while awaiting
// headers)" and killing the download job immediately - the proximate
// cause of "failed to download" for large titles. Raised to give large
// NZBs real headroom without touching the per-segment NNTP timeouts.
var defaultNZBFileFetcher = func() *http.Client {
	client := config.GetHTTPClient(config.TUNNEL_TYPE_NEWZ_NZB_GRAB)
	client.Timeout = 120 * time.Second
	return client
}()

func getNZBFileFetcher(link string) *http.Client {
	if client, err := newznab_indexer.GetHTTPClientForURL(link); err != nil {
		log.Error("failed to get http client for nzb file link, using default client", "error", err, "link", link)
	} else if client != nil {
		return client
	}
	return defaultNZBFileFetcher
}

type OnFetchedHook func(nzbFile *NZBFile, err error, latency time.Duration)

type fetchOptions struct {
	onFetched OnFetchedHook
	indexerId int64
}

type FetchOption func(*fetchOptions)

func WithOnFetched(fn OnFetchedHook) FetchOption {
	return func(o *fetchOptions) { o.onFetched = fn }
}

func WithIndexerId(id int64) FetchOption {
	return func(o *fetchOptions) { o.indexerId = id }
}

func fetchNZBFile(link string, name string, log *logger.Logger, opts *fetchOptions) (*NZBFile, error) {
	clink := util.CleanNZBFileLink(link)
	cacheKey := util.HashNZBFileLink(link)
	nzbFile := &NZBFile{}
	if nzbFileCache.Get(cacheKey, nzbFile) {
		if log != nil {
			log.Debug("fetch nzb - cache hit", "link", clink)
		}
	} else if fetchErr := ""; nzbFetchErrCache.Get(cacheKey, &fetchErr) {
		if log != nil {
			log.Debug("fetch nzb - cached failure", "link", clink)
		}
		return nil, fmt.Errorf("cached failure: %s", fetchErr)
	} else {
		if log != nil {
			log.Debug("fetch nzb - cache miss", "link", clink)
		}
		file, err, _ := nzbFileFetchSG.Do(cacheKey, func() (ret any, err error) {
			startTime := time.Now()
			var file *NZBFile

			defer func() {
				if opts != nil && opts.onFetched != nil {
					opts.onFetched(file, err, time.Since(startTime))
				}
				if err != nil {
					if cacheErr := nzbFetchErrCache.Add(cacheKey, err.Error()); cacheErr != nil && log != nil {
						log.Warn("fetch nzb - failed to cache failure", "error", cacheErr, "link", clink)
					}
				}
			}()

			req, err := http.NewRequest("GET", link, nil)
			if err != nil {
				return nil, err
			}
			req.Header = config.Newz.IndexerRequestHeader.Grab.Clone()
			res, err := getNZBFileFetcher(link).Do(req)
			if err != nil {
				return nil, err
			}
			defer res.Body.Close()

			if res.StatusCode < 200 || 300 <= res.StatusCode {
				return nil, fmt.Errorf("failed to fetch nzb: status %d", res.StatusCode)
			}

			if res.ContentLength > config.Newz.NZBFileMaxSize {
				return nil, fmt.Errorf("file too large: %d bytes (max %d)", res.ContentLength, config.Newz.NZBFileMaxSize)
			}

			blob, err := io.ReadAll(io.LimitReader(res.Body, config.Newz.NZBFileMaxSize+1024))
			if err != nil {
				if log != nil {
					log.Error("fetch nzb - failed", "error", err, "link", clink)
				}
				return nil, err
			}
			if size := int64(len(blob)); size > config.Newz.NZBFileMaxSize {
				return nil, fmt.Errorf("file too large: %d+ bytes (max %d)", size, config.Newz.NZBFileMaxSize)
			}
			if len(blob) == 0 {
				return nil, fmt.Errorf("empty response body")
			}
			if log != nil {
				log.Debug("fetch nzb - completed", "link", clink)
			}

			if name == "" {
				name = "unknown.nzb"
			}
			filename := name
			if cd := res.Header.Get("Content-Disposition"); cd != "" {
				_, params, _ := mime.ParseMediaType(cd)
				if fn := params["filename"]; fn != "" {
					filename = fn
				}
			}
			if filename == name {
				if fn := path.Base(link); strings.HasSuffix(fn, ".nzb") {
					filename = fn
				}
			}
			if !strings.HasSuffix(filename, ".nzb") {
				filename += ".nzb"
			}
			file = &NZBFile{
				Blob: blob,
				Name: filename,
				Link: link,
				Mod:  time.Now(),
			}

			if cacheErr := nzbFileCache.Add(cacheKey, *file); cacheErr != nil && log != nil {
				log.Warn("fetch nzb - failed to cache", "error", cacheErr, "link", clink)
			}
			return file, nil
		})
		if err != nil {
			if log != nil {
				log.Error("fetch nzb - failed", "error", err, "link", clink)
			}
			return nil, err
		}
		nzbFile = file.(*NZBFile)
	}
	return nzbFile, nil
}

func FetchNZBFile(link string, name string, log *logger.Logger, opts ...FetchOption) (*NZBFile, error) {
	options := fetchOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	onFetch := options.onFetched
	indexerId := options.indexerId
	options.onFetched = func(nzbFile *NZBFile, err error, latency time.Duration) {
		if onFetch != nil {
			onFetch(nzbFile, err, latency)
		}
		if nzbFile == nil {
			return
		}
		QueueJob("", nzbFile.Name, nzbFile.Link, "", 0, "", indexerId)
	}
	return fetchNZBFile(link, name, log, &options)
}

func CacheNZBFile(hash string, file NZBFile) error {
	return nzbFileCache.Add(hash, file)
}

func DeleteNZBFile(link string) {
	cacheKey := util.HashNZBFileLink(link)
	nzbFileCache.Remove(cacheKey)
}
