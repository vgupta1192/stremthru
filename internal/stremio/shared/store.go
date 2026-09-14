package stremio_shared

import (
	"time"

	"github.com/MunifTanjim/stremthru/core"
	"github.com/MunifTanjim/stremthru/internal/config"
	"github.com/MunifTanjim/stremthru/internal/stremio/configure"
	"github.com/MunifTanjim/stremthru/store"
)

var P2PEnabled = config.Feature.IsEnabled(config.FeatureStremioP2P)

func GetStoreCodeOptions(includeP2P bool) []configure.ConfigOption {
	options := []configure.ConfigOption{
		{Value: "", Label: "StremThru"},
		{Value: "ad", Label: "AllDebrid"},
		{Value: "dr", Label: "⚠️ Debrider"},
		{Value: "dl", Label: "DebridLink"},
		{Value: "ed", Label: "⚠️ EasyDebrid"},
		{Value: "oc", Label: "Offcloud"},
		{Value: "pm", Label: "Premiumize"},
		{Value: "pp", Label: "PikPak"},
		{Value: "rd", Label: "RealDebrid"},
		{Value: "tb", Label: "TorBox"},
		{Value: "ti", Label: "🧪 Torrin"},
	}
	if config.IsPublicInstance {
		options[0].Disabled = true
		options[0].Label = ""
	}
	if P2PEnabled && includeP2P {
		options = append(options, configure.ConfigOption{
			Value: "p2p",
			Label: "P2P 🧪",
		})
	}
	return options
}

// nextPollDelay ramps up from 1s, doubling each retry, capped at
// retryInterval. A magnet/newz that's already effectively cached on the
// store's side usually flips to the target status within a second or two
// of AddMagnet/AddNewz - polling at a flat retryInterval (previously a
// hardcoded 5s for magnets) meant the common "basically instant" case
// still paid the full interval before the first recheck. Ramping keeps
// the same total worst-case patience budget (maxRetry*retryInterval is
// still the ceiling once the delay saturates at retryInterval) while
// resolving the common fast case in ~1-2s instead of a flat 5s+.
func nextPollDelay(retry int, retryInterval time.Duration) time.Duration {
	delay := time.Second << retry
	if delay > retryInterval || delay <= 0 {
		delay = retryInterval
	}
	return delay
}

func WaitForMagnetStatus(ctx *Ctx, m *store.GetMagnetData, status store.MagnetStatus, maxRetry int, retryInterval time.Duration) (*store.GetMagnetData, error) {
	// most stores only populate Private in AddMagnet (derived from the torrent
	// metainfo), not in GetMagnet, so carry the incoming value across refreshes.
	private := m.Private
	retry := 0
	for m.Status != status && retry < maxRetry {
		gmParams := &store.GetMagnetParams{
			Id:       m.Id,
			ClientIP: ctx.ClientIP,
		}
		gmParams.APIKey = ctx.StoreAuthToken
		magnet, err := ctx.Store.GetMagnet(gmParams)
		if err != nil {
			return m, err
		}
		m = magnet
		private = private || m.Private
		m.Private = private
		if m.Status == status {
			break
		}
		time.Sleep(nextPollDelay(retry, retryInterval))
		retry++
	}
	if m.Status != status {
		error := core.NewStoreError("torrent failed to reach status: " + string(status) + ", last status: " + string(m.Status))
		error.StoreName = string(ctx.Store.GetName())
		return m, error
	}
	return m, nil
}

func GetStoreCodeOptionsForNewz() []configure.ConfigOption {
	options := []configure.ConfigOption{
		{Value: "", Label: "StremThru"},
		{Value: "tb", Label: "TorBox"},
	}
	if config.IsPublicInstance {
		options[0].Disabled = true
		options[0].Label = ""
	}
	return options
}

func isTerminalFailedNewzStatus(status store.NewzStatus) bool {
	switch status {
	case store.NewzStatusFailed, store.NewzStatusInvalid, store.NewzStatusUnknown:
		return true
	default:
		return false
	}
}

// Confirmed live (2026-09-13): this loop only checked for the target status,
// never for a terminal failure - so a genuinely failed/invalid NZB polled
// for the *entire* maxRetry budget before returning an error, exactly
// backwards from the intent (fail fast, wait patiently for real progress).
// Combined with PlaybackWaitTime previously defaulting to 5s (maxRetry=1
// at the usual 5s retryInterval), a click on a title that was still
// downloading got almost no chance to finish before falling back to the
// "downloading" placeholder video, requiring a manual re-click once the
// download had actually completed in the background.
func WaitForNewzStatus(ctx *Ctx, data *store.GetNewzData, status store.NewzStatus, maxRetry int, retryInterval time.Duration) (*store.GetNewzData, error) {
	retry := 0
	for data.Status != status && !isTerminalFailedNewzStatus(data.Status) && retry < maxRetry {
		params := &store.GetNewzParams{
			Id:       data.Id,
			ClientIP: ctx.ClientIP,
		}
		params.APIKey = ctx.StoreAuthToken
		newz, err := ctx.Store.(store.NewzStore).GetNewz(params)
		if err != nil {
			return data, err
		}
		data = newz
		if data.Status == status || isTerminalFailedNewzStatus(data.Status) {
			break
		}
		time.Sleep(nextPollDelay(retry, retryInterval))
		retry++
	}
	if data.Status != status {
		error := core.NewStoreError("newz failed to reach status: " + string(status) + ", last status: " + string(data.Status))
		error.StoreName = string(ctx.Store.GetName())
		return data, error
	}
	return data, nil
}
