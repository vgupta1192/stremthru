package torrent_info

import (
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/MunifTanjim/stremthru/internal/util"
	"github.com/MunifTanjim/stremthru/stremio"
)

var torrentioStreamHashRegex = regexp.MustCompile(`(?i)\/([a-f0-9]{40})\/[^/]+\/(?:(\d+)|null|undefined)\/`)
var torrentioStreamSeedersRegex = regexp.MustCompile(`👤 (\d+)`)
var torrentioStreamSizeRegex = regexp.MustCompile(`💾 (?:([\d.]+ [^ ]+)|.+?)`)
var torrentioDebridTrustedFileIndexRegex = regexp.MustCompile(`\[(?:RD|DL)`)

func isTorrentioDebridFileIndexTrustable(name string) bool {
	return torrentioDebridTrustedFileIndexRegex.MatchString(name)
}

func extractInputFromTorrentioStream(data *TorrentInfoInsertData, sid string, stream *stremio.Stream) *TorrentInfoInsertData {
	description := stream.Description
	if description == "" {
		description = stream.Title
	}
	torrentTitle, descriptionRest, _ := strings.Cut(description, "\n")
	data.TorrentTitle = torrentTitle
	file := TorrentInfoInsertDataFile{
		Idx:  -1,
		Size: -1,
		SId:  sid,
	}

	if stream.BehaviorHints != nil && stream.BehaviorHints.Filename != "" {
		file.Name = stream.BehaviorHints.Filename
	} else if descriptionRest != "" && !strings.HasPrefix(descriptionRest, "👤") {
		file.Name, _, _ = strings.Cut(descriptionRest, "\n")
		file.Name = filepath.Base(file.Name)
	}
	if stream.InfoHash == "" {
		if match := torrentioStreamHashRegex.FindStringSubmatch(stream.URL); len(match) > 0 {
			data.Hash = match[1]
			if isTorrentioDebridFileIndexTrustable(stream.Name) && len(match) > 2 {
				if idx, err := strconv.Atoi(match[2]); err == nil {
					file.Idx = idx
				}
			}
		}
	} else {
		data.Hash = stream.InfoHash
		file.Idx = stream.FileIndex
	}
	if match := torrentioStreamSeedersRegex.FindStringSubmatch(description); len(match) > 1 {
		data.Seeders = util.SafeParseInt(match[1], -1)
	}
	if match := torrentioStreamSizeRegex.FindStringSubmatch(description); len(match) > 1 {
		file.Size = util.ToBytes(match[1])
	}
	if file.Name != "" {
		data.Files = append(data.Files, file)
	}
	data.Size = -1
	return data
}

// Added 2026-09-14: MediaFusion is self-hosted on this box rather than a
// public well-known domain like Torrentio, so it's identified by host:port
// (127.0.0.1:8210) instead of just a hostname - matching on bare
// "127.0.0.1" would also catch any other locally-hosted upstream a user
// adds later and mis-tag its results as MediaFusion's. ExtractCreateDataFromStream
// is called with up.baseUrl.Host (not .Hostname()) specifically so this
// port-qualified match works; Torrentio's public HTTPS URL has no explicit
// port so its existing bare-hostname match is unaffected.
const mediaFusionHost = "127.0.0.1:8210"

func extractInputFromMediaFusionStream(data *TorrentInfoInsertData, sid string, stream *stremio.Stream) *TorrentInfoInsertData {
	description := stream.Description
	if description == "" {
		description = stream.Title
	}
	torrentTitle, _, _ := strings.Cut(description, "\n")
	data.TorrentTitle = torrentTitle

	file := TorrentInfoInsertDataFile{
		Idx:  -1,
		Size: -1,
		SId:  sid,
	}

	// Unlike Torrentio, MediaFusion populates the standard `infoHash` /
	// `fileIdx` stream fields directly (confirmed live 2026-09-14) rather
	// than encoding the hash into the stream URL, so no regex is needed.
	if stream.InfoHash != "" {
		data.Hash = stream.InfoHash
		file.Idx = stream.FileIndex
	}

	if stream.BehaviorHints != nil {
		if stream.BehaviorHints.Filename != "" {
			file.Name = stream.BehaviorHints.Filename
		}
		if stream.BehaviorHints.VideoSize > 0 {
			file.Size = stream.BehaviorHints.VideoSize
		}
	}

	if file.Name != "" {
		data.Files = append(data.Files, file)
	}
	data.Size = -1
	return data
}

func ExtractCreateDataFromStream(hostname string, sid string, stream *stremio.Stream) *TorrentInfoInsertData {
	data := &TorrentInfoInsertData{}
	switch hostname {
	case util.MustDecodeBase64("dG9ycmVudGlvLnN0cmVtLmZ1bg=="):
		data.Source = TorrentInfoSourceTorrentio
		data = extractInputFromTorrentioStream(data, sid, stream)
	case mediaFusionHost:
		data.Source = TorrentInfoSourceMediaFusion
		data = extractInputFromMediaFusionStream(data, sid, stream)
	}
	if data.Hash == "" {
		return nil
	}
	return data
}
