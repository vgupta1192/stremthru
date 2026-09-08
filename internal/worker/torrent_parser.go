package worker

import (
	"slices"
	"time"

	"github.com/MunifTanjim/go-ptt"
	"github.com/MunifTanjim/stremthru/internal/db"
	ti "github.com/MunifTanjim/stremthru/internal/torrent_info"
)

func InitParseTorrentWorker(conf *WorkerConfig) *Worker {
	if err := ti.MarkForReparseBelowVersion(9000); err != nil {
		panic(err)
	}

	var parseTorrentInfo = func(w *Worker, t *ti.TorrentInfo) *ti.TorrentInfo {
		if t.ParserVersion > ptt.Version().Int() {
			return nil
		}

		err := t.ForceParse()
		if err != nil {
			w.Log.Warn("failed to parse", "error", err, "title", t.TorrentTitle)
			return nil
		}

		return t
	}

	// Batch size and inter-chunk sleep were fixed at 500/1s regardless of
	// dialect - fine for SQLite's single-writer limit, but on Postgres
	// (which already takes 10000-row batches elsewhere, see
	// imdb_title/dataset.go) this throttled the parser to ~150-250 rows/sec,
	// well below the rate DMM hashlist + torznab syncing can ingest new
	// rows. The parser worker never caught up (confirmed live: 866k of
	// 1.24M rows unparsed on a Postgres instance, "started" continuously
	// for 4+ hours), so MarkForReparseBelowVersion above kept finding a
	// large backlog on every restart, which is what made restarts slow -
	// same root cause diagnosed for the original migration backfill.
	fetchSize := 5000
	chunkSize := 500
	chunkSleep := 1 * time.Second
	batchSleep := 5 * time.Second
	if db.Dialect == db.DBDialectPostgres {
		fetchSize = 10000
		chunkSize = 10000
		chunkSleep = 100 * time.Millisecond
		batchSleep = 500 * time.Millisecond
	}

	conf.Executor = func(w *Worker) error {
		log := w.Log
		for {
			tInfos, err := ti.GetUnparsed(fetchSize)
			if err != nil {
				return err
			}

			for cTInfos := range slices.Chunk(tInfos, chunkSize) {
				parsedTInfos := []*ti.TorrentInfo{}
				for i := range cTInfos {
					if t := parseTorrentInfo(w, &cTInfos[i]); t != nil {
						parsedTInfos = append(parsedTInfos, t)
					}
				}
				if err := ti.UpsertParsed(parsedTInfos); err != nil {
					return err
				}
				log.Info("upserted parsed torrent info", "count", len(parsedTInfos))
				time.Sleep(chunkSleep)
			}

			if len(tInfos) < fetchSize {
				break
			}

			time.Sleep(batchSleep)
		}

		return nil
	}

	worker := NewWorker(conf)

	return worker
}
