package stremio_wrap

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/MunifTanjim/stremthru/core"
	"github.com/MunifTanjim/stremthru/internal/buddy"
	"github.com/MunifTanjim/stremthru/internal/config"
	"github.com/MunifTanjim/stremthru/internal/shared"
	stremio_addon "github.com/MunifTanjim/stremthru/internal/stremio/addon"
	stremio_torz "github.com/MunifTanjim/stremthru/internal/stremio/torz"
	stremio_transformer "github.com/MunifTanjim/stremthru/internal/stremio/transformer"
	"github.com/MunifTanjim/stremthru/internal/torrent_info"
	"github.com/MunifTanjim/stremthru/internal/torrent_stream"
	torznab_indexer_syncinfo "github.com/MunifTanjim/stremthru/internal/torznab/indexer/syncinfo"
	"github.com/MunifTanjim/stremthru/internal/worker"
	"github.com/MunifTanjim/stremthru/store"
	"github.com/MunifTanjim/stremthru/stremio"
)

var lazyPullTorz = config.Stremio.Torz.LazyPull

func (ud UserData) fetchStream(ctx *Ctx, r *http.Request, rType, id string) (*stremio.StreamHandlerResponse, error) {
	log := ctx.Log

	filter, filter_err := ud.GetFilter()
	if filter_err != nil {
		log.Warn("failed to parse filter expression", "error", filter_err)
		return nil, shared.ErrorBadRequest(r, "invalid filter expression: "+filter_err.Error())
	}

	eud := ud.GetEncoded()

	stremId := strings.TrimSuffix(id, ".json")

	upstreams, err := ud.getUpstreams(ctx, stremio.ResourceNameStream, rType, id)
	if err != nil {
		return nil, err
	}
	upstreamsCount := len(upstreams)
	log.Debug("found addons for stream", "count", upstreamsCount)

	chunksCount := upstreamsCount
	if ud.IncludeTorz {
		chunksCount += 1
	}

	template, err := ud.template.Parse()
	if err != nil {
		return nil, err
	}

	isImdbStremId := strings.HasPrefix(stremId, "tt")
	torrentInfoCategory := torrent_info.GetCategoryFromStremId(stremId, rType)

	nsid, err := torrent_stream.NormalizeStreamId(stremId)
	if err == nil {
		cleanSId := nsid.ToClean()
		if !ud.IncludeTorz || lazyPullTorz {
			go buddy.PullTorrentsByStremId(cleanSId, "")
		} else {
			buddy.PullTorrentsByStremId(cleanSId, "")
		}

		torznab_indexer_syncinfo.QueueJob(nsid.String())
	} else if !errors.Is(err, torrent_stream.ErrUnsupportedStremId) {
		log.Error("failed to normalize strem id", "strem_id", stremId, "error", err)
	} else {
		log.Warn("unsupported strem id for normalization", "strem_id", stremId)
	}

	chunkIdxOffset := 0
	// Added 2026-09-19: upstream fetches now hand their result back through a
	// channel and the request only waits a bounded window (below) for them.
	// Previously wg.Wait() held every link click hostage to the slowest
	// source - confirmed live on a cold title: MediaFusion's first scrape
	// exceeded the HTTP timeout, the request waited the full 15s for it,
	// and it still contributed nothing ("failed to fetch streams ... context
	// deadline exceeded"), before CheckMagnet even started. With the
	// channel+deadline, stragglers are simply skipped for THIS response -
	// their goroutine keeps running to completion (on a detached context),
	// and a successful fetch still populates FetchStream's 8h response
	// cache (and torrent_info upserts), so the very next click on the same
	// title gets those results instantly. Skipped chunks are simply absent
	// from that first response.
	type chunkResult struct {
		idx     int
		streams []WrappedStream
		err     error
	}
	chunks := make([]chunkResult, chunksCount)
	chunkResults := make(chan chunkResult, chunksCount)
	// How long the response waits for all sources to come back. Generous
	// enough that warm/fast sources always make it (measured: torz cache
	// read ~10ms, Torrentio warm ~1-3s, MediaFusion warm sub-second), tight
	// enough that a genuinely stalled source costs seconds, not minutes.
	// Lowered 6s -> 4s (2026-09-19, second pass): with the deadline at 6s
	// the worst-case cold click measured ~25s (6s wait + 15s CheckMagnet
	// chunk deadline + overhead) - the wait itself was pure dead time for
	// sources that had already missed the window.
	upstreamCollectDeadline := 4 * time.Second
	deliverChunk := func(idx int, streams []WrappedStream, err error) {
		chunkResults <- chunkResult{idx: idx, streams: streams, err: err}
	}
	if ud.IncludeTorz {
		chunkIdxOffset = 1
		go func() {
			hashes, err := torrent_info.ListHashesByStremId(stremId)
			if err != nil {
				if errors.Is(err, torrent_stream.ErrUnsupportedStremId) {
					deliverChunk(0, nil, nil)
					return
				}

				deliverChunk(0, nil, err)
				return
			}

			if nsid == nil {
				deliverChunk(0, nil, nil)
				return
			}

			streams, err := stremio_torz.GetStreamsForHashes(rType, stremId, hashes, nsid)
			if err != nil {
				deliverChunk(0, nil, err)
				return
			}

			// Added 2026-09-14: previously this only ever read already-cached
			// results - wrap had no indexer list at all, so it could never do
			// a live Jackett search of its own for the torz-included portion,
			// unlike the standalone torz addon. Runs the same live search
			// torz itself does, merged in alongside the cache read.
			//
			// skip_live=1 (added for catalog_warmer.py) was meant to opt out
			// of only the *redundant* background refresh below, on the
			// assumption that "the queued sync job runs and fills cache
			// either way, live search or not" - but there is no other job:
			// ListHashesByStremId/GetStreamsForHashes above are pure reads
			// of already-crawled data, so for a title with zero existing
			// hashes (a genuine first-ever search - the common case for
			// long-tail/newly-added catalogue entries, not popular titles),
			// the live search a few lines down is the *only* thing that
			// ever populates the cache. The original code gated both
			// branches on skip_live, so a warmer request for such a title
			// took neither branch, came back with 0 streams, and got marked
			// "warmed" anyway (see catalog_warmer.py's state.json, which
			// then skips it for --refresh-days) - silently defeating the
			// warmer for exactly the titles it most needs to pre-cache.
			// Fixed by only gating the background-refresh branch (the
			// truly redundant one, for titles that already have hashes) on
			// skip_live; the synchronous first-ever-search branch always
			// runs regardless, matching what its own comment below already
			// promised.
			if len(ctx.Indexers) > 0 {
				// Confirmed live (2026-09-14): a 15-title real-world test
				// showed several popular titles taking 45-60s+ because this
				// live search ran and was waited on even when the cache
				// read above had already turned up plenty of streams to
				// show - the request was held hostage by a search it didn't
				// need. If the cache already has something, return it
				// immediately and run the live search detached in the
				// background instead, purely to keep the cache fresh for
				// next time; GetStreamsFromIndexers persists whatever it
				// finds via its own `go torrent_info.Upsert(...)` regardless
				// of whether anything reads its return value, so nothing is
				// lost by not waiting on it here.
				//
				// A stremId with no cached hashes at all (first-ever
				// search) used to always wait on live search here, since it
				// was the only source of results for that request - but
				// that's exactly the case that could occasionally run the
				// full IndexerMaxTimeout (45s) when a normally-fast indexer
				// has a slow moment (confirmed live, 2026-09-16: jackett/
				// uindex hit 44.7s on a real request despite testing
				// sub-20ms in the periodic health check). skip_live_torz
				// backgrounds this case too instead of blocking on it - the
				// request returns immediately with whatever upstream
				// addons (Torrentio/MediaFusion) have, and the indexer
				// search still runs and populates the cache for the very
				// next click on the same title, typically within seconds
				// rather than waiting on the separately-scheduled
				// sync-torznab-indexer job's own cadence (every 5-10min).
				// Only a genuine cache-miss on a non-opted-in key still
				// blocks, since live search is that request's only
				// possible source of torz results at all.
				bgCtx := &stremio_torz.Ctx{Ctx: ctx.Ctx, Indexers: ctx.Indexers}
				runInBackground := func() {
					if r.URL.Query().Get("skip_live") == "1" {
						return
					}
					go func() {
						timeoutCtx, cancel := context.WithTimeout(context.Background(), config.Stremio.Torz.IndexerMaxTimeout)
						defer cancel()
						start := time.Now()
						bgStreams, bgHashes, err := stremio_torz.GetStreamsFromIndexers(timeoutCtx, bgCtx, rType, stremId)
						if err != nil {
							log.Error("failed to fetch live torz streams (background refresh)", "error", err)
							return
						}
						// Only logging site for this path (2026-09-16) -
						// GetStreamsFromIndexers itself is silent on
						// success, so without this there was no way to
						// tell "ran and found nothing new" apart from
						// "never ran at all" from the logs alone. Directly
						// caused real confusion diagnosing skip_live_torz.
						log.Info("background torz refresh done", "strem_id", stremId, "streams", len(bgStreams), "hashes", len(bgHashes), "duration", time.Since(start))
					}()
				}
				if len(streams) > 0 {
					runInBackground()
				} else if ud.SkipLiveTorz {
					runInBackground()
				} else {
					timeoutCtx, cancel := context.WithTimeout(r.Context(), config.Stremio.Torz.IndexerMaxTimeout)
					liveStreams, _, liveErr := stremio_torz.GetStreamsFromIndexers(timeoutCtx, &stremio_torz.Ctx{Ctx: ctx.Ctx, Indexers: ctx.Indexers}, rType, stremId)
					cancel()
					if liveErr != nil {
						log.Error("failed to fetch live torz streams", "error", liveErr)
					} else {
						streams = append(streams, liveStreams...)
					}
				}
			}

			wstreams := make([]WrappedStream, len(streams))
			for i := range streams {
				wstream := &streams[i]
				stream := wstream.Stream
				tmpl := template
				if tmpl == nil || tmpl.IsEmpty() || tmpl.IsRaw() {
					tmpl = stremio_transformer.StreamTemplateDefault
				}
				s, err := tmpl.Execute(stream, wstream.R)
				if err != nil {
					deliverChunk(0, nil, err)
					return
				}
				wstreams[i] = WrappedStream{
					Stream: s,
					r:      wstream.R,
				}
			}
			deliverChunk(0, wstreams, nil)
		}()
	}
	for i := range upstreams {
		idx := i + chunkIdxOffset
		go func() {
			up := &upstreams[i]
			// Detached context (2026-09-19): this fetch must be able to
			// outlive the HTTP request that started it - when the request's
			// 6s collect window expires, the goroutine keeps running so its
			// successful response still lands in FetchStream's cache for
			// the next click. 45s is a hard stop well inside reason.
			fetchCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			fetchParams := &stremio_addon.FetchStreamParams{
				BaseURL:  up.baseUrl,
				Type:     rType,
				Id:       id,
				ClientIP: ctx.ClientIP,
			}
			fetchParams.Context = fetchCtx
			res, err := addon.FetchStream(fetchParams)
			streams := res.Data.Streams
			wstreams := make([]WrappedStream, len(streams))
			tInfos := []torrent_info.TorrentInfoInsertData{}
			if err == nil {
				extractor, err := up.extractor.Parse()
				if err != nil {
					deliverChunk(idx, nil, err)
					return
				}
				// .Host (not .Hostname()) so self-hosted upstreams on a
				// non-default port (MediaFusion, 127.0.0.1:8210) are
				// identified precisely - see the comment on
				// mediaFusionHost in torrent_info/extractor.go.
				addonHostname := up.baseUrl.Host
				transformer := StreamTransformer{
					Extractor: extractor,
					Template:  template,
				}
				for i := range streams {
					stream := streams[i]
					if isImdbStremId {
						if cData := torrent_info.ExtractCreateDataFromStream(addonHostname, stremId, &stream); cData != nil {
							tInfos = append(tInfos, *cData)
						}
					}
					wstream, err := transformer.Do(&stream, rType, up.ReconfigureStore)
					if err != nil {
						LogError(r, "failed to transform stream", err)
					}
					if up.NoContentProxy {
						wstream.noContentProxy = true
					}
					wstreams[i] = *wstream
				}
			}
			if isImdbStremId {
				if len(tInfos) > 0 {
					worker.TorrentPusherQueue.Queue(stremId)
				}
				go torrent_info.Upsert(tInfos, torrentInfoCategory, false)
			}
			deliverChunk(idx, wstreams, err)
		}()
	}

	received := 0
	collectDeadline := time.After(upstreamCollectDeadline)
	for received < chunksCount {
		select {
		case res := <-chunkResults:
			chunks[res.idx] = res
			received++
		case <-collectDeadline:
			received = chunksCount
		}
	}

	allStreams := []WrappedStream{}
	if ud.IncludeTorz {
		if chunks[0].err != nil {
			log.Error("failed to fetch torz streams", "error", chunks[0].err)
		} else {
			allStreams = append(allStreams, chunks[0].streams...)
		}
	}
	for i := range upstreams {
		idx := i + chunkIdxOffset
		hostname := upstreams[i].baseUrl.Hostname()
		if chunks[idx].err != nil {
			log.Error("failed to fetch streams", "error", chunks[idx].err, "hostname", hostname)
		} else {
			allStreams = append(allStreams, chunks[idx].streams...)
		}
	}

	if ud.IncludeTorz {
		allStreams = dedupeStreams(allStreams)
	}

	allStreams = filterStreams(allStreams, filter)

	if template != nil {
		stremio_transformer.SortStreams(allStreams, ud.Sort)
	}

	if !ud.IncludeTorz {
		allStreams = dedupeStreams(allStreams)
	}

	totalStreams := len(allStreams)
	log.Debug("found streams", "total_count", totalStreams, "deduped_count", len(allStreams))

	hashes := []string{}
	magnetByHash := map[string]core.MagnetLink{}
	for i := range allStreams {
		stream := &allStreams[i]
		if (stream.URL == "" || strings.HasPrefix(stream.URL, "magnet:?")) && stream.InfoHash != "" {
			magnet, err := core.ParseMagnetLink(stream.InfoHash)
			if err != nil {
				continue
			}
			hashes = append(hashes, magnet.Hash)
			magnetByHash[magnet.Hash] = magnet
		}
	}

	isCachedByHash := map[string]string{}
	hasErrByStoreCode := map[string]struct{}{}
	if len(hashes) > 0 {
		cmRes := ud.CheckMagnet(&store.CheckMagnetParams{
			Magnets:  hashes,
			ClientIP: ctx.ClientIP,
			SId:      stremId,
		}, log)
		if cmRes.HasErr && len(cmRes.ByHash) == 0 {
			return nil, errors.Join(cmRes.Err...)
		}
		isCachedByHash = cmRes.ByHash
		hasErrByStoreCode = cmRes.HasErrByStoreCode
	}

	cachedStreams := []stremio.Stream{}
	uncachedStreams := []stremio.Stream{}
	for i := range allStreams {
		stream := &allStreams[i]
		if (stream.URL == "" || strings.HasPrefix(stream.URL, "magnet:?")) && stream.InfoHash != "" {
			magnet, ok := magnetByHash[strings.ToLower(stream.InfoHash)]
			if !ok {
				continue
			}
			surl := shared.ExtractRequestBaseURL(r).JoinPath("/stremio/wrap/" + eud + "/_/strem/" + magnet.Hash + "/" + strconv.Itoa(stream.FileIndex) + "/")
			if stream.BehaviorHints != nil && stream.BehaviorHints.Filename != "" {
				surl = surl.JoinPath(url.PathEscape(stream.BehaviorHints.Filename))
			}
			surl.RawQuery = "sid=" + stremId
			if stream.r != nil && stream.r.Season != -1 && stream.r.Episode != -1 {
				surl.RawQuery += "&re=" + url.QueryEscape(strconv.Itoa(stream.r.Season)+".{1,3}"+strconv.Itoa(stream.r.Episode))
			}
			stream.InfoHash = ""
			stream.FileIndex = 0

			storeCode, ok := isCachedByHash[magnet.Hash]
			if ok && storeCode != "" {
				surl.RawQuery += "&s=" + storeCode
				stream.URL = surl.String()
				stream.Name = "⚡ [" + storeCode + "] " + stream.Name

				if ctx.IsProxyAuthorized && config.StoreContentProxy.IsEnabled(string(ud.GetStoreByCode(storeCode).Store.GetName())) {
					stream.Name = "✨ " + stream.Name
				}

				cachedStreams = append(cachedStreams, *stream.Stream)
			} else if !ud.CachedOnly && (stream.r == nil || !stream.r.IsPrivate) {
				surlRawQuery := surl.RawQuery
				stores := ud.GetStores()
				for i := range stores {
					s := &stores[i]
					storeName := s.Store.GetName()
					storeCode := strings.ToUpper(string(storeName.Code()))
					if _, hasErr := hasErrByStoreCode[storeCode]; hasErr || storeCode == "ED" {
						continue
					}

					stream := *stream.Stream
					surl.RawQuery = surlRawQuery + "&s=" + storeCode
					stream.URL = surl.String()
					stream.Name = "[" + storeCode + "] " + stream.Name

					if ctx.IsProxyAuthorized && config.StoreContentProxy.IsEnabled(string(storeName)) && ctx.IsProxyAuthorized {
						stream.Name = "✨ " + stream.Name
					}

					uncachedStreams = append(uncachedStreams, stream)
				}
			}
		} else if stream.URL != "" {
			if !stream.noContentProxy {
				var headers map[string]string
				if stream.BehaviorHints != nil && stream.BehaviorHints.ProxyHeaders != nil && stream.BehaviorHints.ProxyHeaders.Request != nil {
					headers = stream.BehaviorHints.ProxyHeaders.Request
				}

				if ctx.IsProxyAuthorized {
					if url, err := shared.CreateProxyLink(r, stream.URL, headers, config.TUNNEL_TYPE_AUTO, 12*time.Hour, ctx.ProxyAuthUser, ctx.ProxyAuthPassword, true, ""); err == nil && url != stream.URL {
						stream.URL = url
						stream.Name = "✨ " + stream.Name
					}
				}
			}
			if stream.r == nil || stream.r.Store.IsCached {
				cachedStreams = append(cachedStreams, *stream.Stream)
			} else {
				uncachedStreams = append(uncachedStreams, *stream.Stream)
			}
		}
	}

	streams := make([]stremio.Stream, len(cachedStreams)+len(uncachedStreams))
	idx := 0
	for i := range cachedStreams {
		streams[idx] = cachedStreams[i]
		idx++
	}
	for i := range uncachedStreams {
		streams[idx] = uncachedStreams[i]
		idx++
	}

	return &stremio.StreamHandlerResponse{
		Streams: streams,
	}, nil
}

func filterStreams(streams []WrappedStream, filter *stremio_transformer.StreamFilter) []WrappedStream {
	if filter == nil || filter.IsEmpty() {
		return streams
	}
	result := make([]WrappedStream, 0, len(streams))
	for i := range streams {
		stream := &streams[i]
		if stream.r == nil || filter.Match(stream.r) {
			result = append(result, *stream)
		}
	}
	return result
}
