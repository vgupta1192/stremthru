package dash_api

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/MunifTanjim/stremthru/internal/logger"
	"github.com/MunifTanjim/stremthru/internal/server"
	torznab_indexer "github.com/MunifTanjim/stremthru/internal/torznab/indexer"
)

var jackettLog = logger.Scoped("dash/jackett")

// These live outside stremthru's own config/DB - they're seedbox-specific
// automation (indexer_health_check.py, catalog_warmer.py) that also needs
// to know the current Jackett API key. Kept in sync here so a key change
// made through this panel doesn't leave them on a stale copy.
func jackettServerConfigPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".apps", "jackett", "Jackett", "ServerConfig.json")
}

func catalogWarmerEnvPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "seedbox-config", "scripts", "catalog-warmer.env")
}

func indexerHealthStatePath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "seedbox-config", "scripts", "indexer-health-state.json")
}

type GetJackettAPIKeyData struct {
	APIKey string `json:"api_key"`
}

func handleGetJackettAPIKey(w http.ResponseWriter, r *http.Request) {
	// Read straight from Jackett's own config - this is only ever a
	// convenience prefill so the panel shows what Jackett currently has
	// configured. The actual key still lives there; this endpoint never
	// writes to it.
	blob, err := os.ReadFile(jackettServerConfigPath())
	if err != nil {
		SendError(w, r, err)
		return
	}
	var cfg struct {
		APIKey string `json:"APIKey"`
	}
	if err := json.Unmarshal(blob, &cfg); err != nil {
		SendError(w, r, err)
		return
	}
	SendData(w, r, 200, GetJackettAPIKeyData{APIKey: cfg.APIKey})
}

type UpdateJackettAPIKeyRequest struct {
	APIKey string `json:"api_key"`
}

type UpdateJackettAPIKeyData struct {
	UpdatedIndexers int  `json:"updated_indexers"`
	Restarting      bool `json:"restarting"`
}

func syncCatalogWarmerEnv(apiKey string) error {
	path := catalogWarmerEnvPath()
	blob, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(blob), "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "JACKETT_API_KEY=") {
			lines[i] = "JACKETT_API_KEY=" + apiKey
		}
	}
	return os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0644)
}

// Prevents indexer_health_check.py's own rotation detection from also
// firing (and redundantly restarting stremthru again) on its next run,
// right after this endpoint already propagated the same new key.
func syncIndexerHealthState(apiKey string) error {
	path := indexerHealthStatePath()
	state := map[string]any{}
	if blob, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(blob, &state)
	}
	state["_jackett_api_key"] = apiKey
	blob, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, blob, 0644)
}

func handleUpdateJackettAPIKey(w http.ResponseWriter, r *http.Request) {
	req := &UpdateJackettAPIKeyRequest{}
	if err := ReadRequestBodyJSON(r, req); err != nil {
		SendError(w, r, err)
		return
	}
	apiKey := strings.TrimSpace(req.APIKey)
	if apiKey == "" {
		ErrorBadRequest(r).Append(Error{
			Location:     "api_key",
			LocationType: server.LocationTypeBody,
			Message:      "api_key is required",
		}).Send(w, r)
		return
	}

	indexers, err := torznab_indexer.GetAll()
	if err != nil {
		SendError(w, r, err)
		return
	}

	updated := 0
	for i := range indexers {
		idx := &indexers[i]
		if idx.Type != torznab_indexer.IndexerTypeJackett {
			continue
		}
		if err := idx.SetAPIKey(apiKey); err != nil {
			SendError(w, r, err)
			return
		}
		if err := idx.Update(); err != nil {
			SendError(w, r, err)
			return
		}
		updated++
	}

	if err := syncCatalogWarmerEnv(apiKey); err != nil {
		jackettLog.Warn("failed to sync catalog-warmer.env", "error", err)
	}
	if err := syncIndexerHealthState(apiKey); err != nil {
		jackettLog.Warn("failed to sync indexer-health-state.json", "error", err)
	}

	SendData(w, r, 200, UpdateJackettAPIKeyData{UpdatedIndexers: updated, Restarting: true})

	// stremthru restarts itself here, so this has to happen after the
	// response is sent - the handler's own process is what's being killed.
	go func() {
		time.Sleep(500 * time.Millisecond)
		if err := exec.Command("systemctl", "--user", "restart", "stremthru").Run(); err != nil {
			jackettLog.Error("failed to restart stremthru after jackett key update", "error", err)
		}
	}()
}

func AddJackettEndpoints(router *http.ServeMux) {
	authed := EnsureAuthed

	router.HandleFunc("/jackett/api-key", authed(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetJackettAPIKey(w, r)
		case http.MethodPut, http.MethodPost:
			handleUpdateJackettAPIKey(w, r)
		default:
			ErrorMethodNotAllowed(r).Send(w, r)
		}
	}))
}
