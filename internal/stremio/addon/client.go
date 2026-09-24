package stremio_addon

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/MunifTanjim/stremthru/core"
	"github.com/MunifTanjim/stremthru/internal/cache"
	"github.com/MunifTanjim/stremthru/internal/config"
	"github.com/MunifTanjim/stremthru/internal/request"
	"github.com/MunifTanjim/stremthru/internal/shared"
	"github.com/MunifTanjim/stremthru/internal/util"
	"github.com/MunifTanjim/stremthru/stremio"
	"golang.org/x/sync/singleflight"
)

var DefaultHTTPClient = func() *http.Client {
	transport := config.DefaultHTTPTransport.Clone()
	return &http.Client{
		Transport: transport,
		// Lowered 2026-09-19 from 30s: wrap stream requests fetch every
		// configured upstream addon in parallel and wait for ALL of them
		// (wg.Wait in wrap/stream.go) before running CheckMagnet, so one
		// slow public addon (Torrentio stalls/rate-limits regularly) held
		// every click hostage for up to 30s. Upstream addons are re-fetched
		// live on every click, so an addon that can't answer in 15s just
		// contributes nothing to THIS click and its results appear on the
		// next one - the right trade for interactive link lists.
		Timeout: 15 * time.Second,
	}
}()

type ClientConfig struct {
	HTTPClient *http.Client
}

type Client struct {
	HTTPClient *http.Client

	reqQuery  func(query *url.Values, params request.Context)
	reqHeader func(query *http.Header, params request.Context)
}

var commonHeaders = func() map[string]string {
	encodedHeaders := map[string]string{
		"UmVmZXJlcg==":     "aHR0cHM6Ly93ZWIuc3RyZW1pby5jb20v",
		"VXNlci1BZ2VudA==": "TW96aWxsYS81LjAgKE1hY2ludG9zaDsgSW50ZWwgTWFjIE9TIFggMTBfMTVfNykgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzEzNC4wLjAuMCBTYWZhcmkvNTM3LjM2",
	}
	headers := map[string]string{}
	for k, v := range encodedHeaders {
		key, err := util.Base64Decode(k)
		if err != nil {
			panic(err)
		}
		val, err := util.Base64Decode(v)
		if err != nil {
			panic(err)
		}
		headers[key] = val
	}
	return headers
}()

func NewClient(conf *ClientConfig) *Client {
	if conf.HTTPClient == nil {
		conf.HTTPClient = DefaultHTTPClient
	}

	c := &Client{}

	c.HTTPClient = conf.HTTPClient

	c.reqQuery = func(query *url.Values, params request.Context) {
	}

	c.reqHeader = func(header *http.Header, params request.Context) {
		for k, v := range commonHeaders {
			header.Set(k, v)
		}
	}

	return c
}

type Ctx = request.Ctx

type ResponseError struct {
	Body       string `json:"body"`
	StatusCode int    `json:"status_code"`
}

func (e *ResponseError) Error() string {
	ret, _ := json.Marshal(e)
	return string(ret)
}

func processResponseBody(res *http.Response, err error, v any) error {
	if err != nil {
		return err
	}

	contentType := res.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		err := core.NewAPIError("unxpected content-type: " + contentType)
		err.StatusCode = res.StatusCode
		return err
	}

	body, err := io.ReadAll(res.Body)
	defer res.Body.Close()

	if err != nil {
		return err
	}

	if res.StatusCode >= 400 {
		return &ResponseError{
			Body:       string(body),
			StatusCode: res.StatusCode,
		}
	}

	return core.UnmarshalJSON(res.StatusCode, body, v)
}

func (c Client) Request(method string, url *url.URL, params request.Context, v any) (*http.Response, error) {
	if params == nil {
		params = &Ctx{}
	}
	req, err := params.NewRequest(url, method, "", c.reqHeader, c.reqQuery)
	if err != nil {
		error := core.NewAPIError("failed to create request")
		error.Cause = err
		return nil, error
	}
	res, err := c.HTTPClient.Do(req)
	err = processResponseBody(res, err, v)
	if err != nil {
		error := core.NewUpstreamError("")
		if rerr, ok := err.(*core.Error); ok {
			error.Msg = rerr.Msg
			error.Code = rerr.Code
			error.StatusCode = rerr.StatusCode
			error.UpstreamCause = rerr
		} else {
			error.Cause = err
		}
		error.InjectReq(req)
		return res, err
	}
	return res, nil
}

func adjustClientIPHeader(params request.Ctx, clientIp string, r *http.Request) {
	if clientIp == "" {
		if r != nil {
			r.Header.Del("X-Client-Ip")
			r.Header.Del("X-Forwarded-For")
			r.Header.Del("X-Real-IP")
		}
		return
	}

	if params.Headers == nil {
		params.Headers = &http.Header{}
	}

	if clientIp == "" {
		params.Headers.Del("X-Client-Ip")
		params.Headers.Del("X-Forwarded-For")
		params.Headers.Del("X-Real-IP")
	} else {
		params.Headers.Set("X-Client-Ip", clientIp)
		params.Headers.Set("X-Forwarded-For", clientIp)
		params.Headers.Set("X-Real-IP", clientIp)
	}
}

type GetManifestParams struct {
	request.Ctx
	BaseURL  *url.URL
	ClientIP string
}

func (c Client) GetManifest(params *GetManifestParams) (request.APIResponse[stremio.Manifest], error) {
	adjustClientIPHeader(params.Ctx, params.ClientIP, nil)
	response := &stremio.Manifest{}
	res, err := c.Request("GET", params.BaseURL.JoinPath("manifest.json"), params, response)
	if err == nil && !response.IsValid() {
		err = errors.New("invalid manifest")
	}
	return request.NewAPIResponse(res, *response), err
}

type FetchStreamParams struct {
	request.Ctx
	BaseURL  *url.URL
	Type     string
	Id       string
	ClientIP string
}

var fetchStreamGroup singleflight.Group

// Unlike stremthru's own torz results (cached by hash in the magnet_cache/
// torrent_info tables), a request for an upstream addon's (e.g. Torrentio)
// stream response had no caching at all before this - singleflight only
// dedupes truly concurrent in-flight requests, not repeat requests over
// time, so every single stream open re-did a full live HTTP call to the
// upstream addon regardless of how recently the same title was searched.
// This is the main remaining reason a repeat search could still feel "live"
// even after stremthru's own cache was warm and fixed. Short TTL (rather
// than something closer to stremthru's own cache) because upstream addons
// like Torrentio return their own debrid-backed stream URLs directly (not
// just magnet hashes) - those can go stale on the upstream's own side, so
// this favors staying fresh over maximizing hit rate. Only successful (200)
// responses are cached; errors/timeouts always retry live next time.
//
// Raised from 7 minutes to 8 hours (2026-09-15): confirmed live that the
// catalog_warmer.py background warmer only re-touches each title every 3
// days, so a 7-minute window meant almost every actual click - even on a
// title the warmer had just "cached" - still paid one live Torrentio
// fetch (confirmed live: ~14.6s for one such fetch) before this cache
// caught up for the rest of that viewing session. For a personal single-
// user seedbox the staleness risk this guards against (a cached
// Torrentio stream URL going dead on Torrentio's own side) is low enough
// that trading some freshness for actually-consistent speed across a
// normal viewing session/day is the better default.
//
// Raised 8h -> 24h (2026-09-19, third pass): with the wrap's bounded 4s
// collect, a cold-at-click-time upstream (MediaFusion cold scrape measured
// 18.75s live) simply doesn't make the window, so first clicks showed
// almost exclusively Torrentio links (measured 108/111 on a trending
// title) whenever the warmer's last touch was >8h old - i.e. nearly
// always under the warmer's multi-day refresh cadence. 24h aligns this
// cache with the warmer's new daily cadence (refresh-days=1), so a
// warmed title stays balanced across ALL sources for the whole interval
// between warmer runs. Staleness trade is the same one already accepted
// in 2026-09-15, just at daily rather than hourly scale; MediaFusion's
// own stream URLs are self-hosted and stable, and Torrentio's are
// re-wrapped through stremthru's proxy at play time anyway.
var fetchStreamCache = cache.NewCache[request.APIResponse[stremio.StreamHandlerResponse]](&cache.CacheConfig{
	Name:     "stremio_addon:fetch_stream",
	Lifetime: 24 * time.Hour,
	MaxSize:  8192,
})

func (c Client) FetchStream(params *FetchStreamParams) (request.APIResponse[stremio.StreamHandlerResponse], error) {
	path := "stream/" + params.Type + "/" + params.Id
	url := params.BaseURL.JoinPath(path)
	cacheKey := url.String()

	var cached request.APIResponse[stremio.StreamHandlerResponse]
	if fetchStreamCache.Get(cacheKey, &cached) {
		return cached, nil
	}

	apiResponse, err, _ := fetchStreamGroup.Do(cacheKey, func() (any, error) {
		adjustClientIPHeader(params.Ctx, params.ClientIP, nil)
		response := &stremio.StreamHandlerResponse{}
		res, err := c.Request("GET", url, params, response)
		apiResponse := request.NewAPIResponse(res, *response)
		if err == nil && apiResponse.StatusCode == http.StatusOK {
			fetchStreamCache.Add(cacheKey, apiResponse)
		}
		return apiResponse, err
	})
	return apiResponse.(request.APIResponse[stremio.StreamHandlerResponse]), err
}

type FetchCatalogParams struct {
	request.Ctx
	BaseURL  *url.URL
	Type     string
	Id       string
	Extra    string
	ClientIP string
}

func (c Client) FetchCatalog(params *FetchCatalogParams) (request.APIResponse[stremio.CatalogHandlerResponse], error) {
	path := "catalog/" + params.Type + "/" + params.Id
	if params.Extra != "" {
		path = path + "/" + params.Extra
	}
	adjustClientIPHeader(params.Ctx, params.ClientIP, nil)
	response := &stremio.CatalogHandlerResponse{}
	res, err := c.Request("GET", params.BaseURL.JoinPath(path), params, response)
	return request.NewAPIResponse(res, *response), err
}

type FetchMetaParams struct {
	request.Ctx
	BaseURL  *url.URL
	Type     string
	Id       string
	ClientIP string
}

func (c Client) FetchMeta(params *FetchMetaParams) (request.APIResponse[stremio.MetaHandlerResponse], error) {
	path := "meta/" + params.Type + "/" + params.Id
	adjustClientIPHeader(params.Ctx, params.ClientIP, nil)
	response := &stremio.MetaHandlerResponse{}
	res, err := c.Request("GET", params.BaseURL.JoinPath(path), params, response)
	return request.NewAPIResponse(res, *response), err
}

type FetchSubtitlesParams struct {
	request.Ctx
	BaseURL  *url.URL
	Type     string
	Id       string
	Extra    string
	ClientIP string
}

func (c Client) FetchSubtitles(params *FetchSubtitlesParams) (request.APIResponse[stremio.SubtitlesHandlerResponse], error) {
	path := "subtitles/" + params.Type + "/" + params.Id
	if params.Extra != "" {
		path = path + "/" + params.Extra
	}
	adjustClientIPHeader(params.Ctx, params.ClientIP, nil)
	response := &stremio.SubtitlesHandlerResponse{}
	res, err := c.Request("GET", params.BaseURL.JoinPath(path), params, response)
	return request.NewAPIResponse(res, *response), err
}

type ProxyResourceParams struct {
	request.Ctx
	BaseURL  *url.URL
	Resource string
	Type     string
	Id       string
	Extra    string
	ClientIP string
}

func (c Client) ProxyResource(w http.ResponseWriter, r *http.Request, params *ProxyResourceParams) {
	path := params.Resource + "/" + params.Type + "/" + params.Id
	if params.Extra != "" {
		path = path + "/" + params.Extra
	}
	adjustClientIPHeader(params.Ctx, params.ClientIP, r)
	w.Header().Del("Access-Control-Allow-Origin")
	shared.ProxyResponse(w, r, params.BaseURL.JoinPath(path).String(), config.TUNNEL_TYPE_AUTO)
}

func NormalizeManifestURL(manifestUrl string) (string, error) {
	u, err := url.Parse(manifestUrl)
	if err != nil {
		return manifestUrl, err
	}
	if u.Scheme == "stremio" {
		u.Scheme = "https"
	}
	if strings.HasSuffix(u.Path, "/configure") {
		u.Path = strings.TrimSuffix(u.Path, "/configure") + "/manifest.json"
	}
	return u.String(), nil
}

func ExtractBaseURL(manifestUrl string) (*url.URL, error) {
	u, err := url.Parse(manifestUrl)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(u.Path, "/manifest.json") {
		return nil, errors.New("invalid manifest url")
	}
	if u.RawPath != "" && u.RawPath != u.Path {
		u.RawPath = strings.TrimSuffix(u.RawPath, "/manifest.json")
		if u.Path, err = url.PathUnescape(u.RawPath); err != nil {
			return nil, err
		}
	} else {
		u.Path = strings.TrimSuffix(u.Path, "/manifest.json")
	}
	return u, nil
}
