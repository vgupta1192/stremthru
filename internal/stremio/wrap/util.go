package stremio_wrap

import (
	"github.com/MunifTanjim/stremthru/internal/shared"
	stremio_shared "github.com/MunifTanjim/stremthru/internal/stremio/shared"
	torznab_client "github.com/MunifTanjim/stremthru/internal/torznab/client"
)

// Confirmed live (2026-09-14): wrap's IncludeTorz path only ever read
// already-cached torz results (GetStreamsForHashes) - it never had an
// Indexers field at all, so it could never do a live Jackett search of its
// own for the torz-included portion, unlike the standalone torz addon.
// Promoted from a type alias to a real struct (mirroring torz.Ctx) so wrap
// can carry a resolved indexer list through the request the same way.
type Ctx struct {
	stremio_shared.Ctx
	Indexers []torznab_client.Indexer
}

var IsMethod = shared.IsMethod
var SendError = shared.SendError
var ExtractRequestBaseURL = shared.ExtractRequestBaseURL

var SendResponse = stremio_shared.SendResponse
var SendHTML = stremio_shared.SendHTML

func dedupeStreams(allStreams []WrappedStream) []WrappedStream {
	hashSeen := map[string]struct{}{}

	streams := []WrappedStream{}
	for i := range allStreams {
		s := allStreams[i]
		if s.r != nil && s.r.Hash != "" {
			if _, seen := hashSeen[s.r.Hash]; seen {
				continue
			} else {
				hashSeen[s.r.Hash] = struct{}{}
			}
		}
		streams = append(streams, s)
	}
	return streams
}
