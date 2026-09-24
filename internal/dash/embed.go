package dash

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"

	"github.com/MunifTanjim/stremthru/internal/server"
)

//go:embed fs/**
var spaFS embed.FS

func GetFileHandler() http.Handler {
	dashFS, err := fs.Sub(spaFS, "fs")
	if err != nil {
		panic(err)
	}
	handler := http.FileServerFS(dashFS)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := server.GetReqCtx(r)
		ctx.NoRequestLog = true

		if strings.HasPrefix(r.URL.Path, "/assets/") {
			w.Header().Set("Cache-Control", "public, max-age=86400")
		} else {
			// _shell.html references content-hashed asset filenames that
			// change on every rebuild - without an explicit no-cache here,
			// browsers can heuristically cache this response and keep
			// loading a shell that points at assets which no longer exist
			// after a redeploy, breaking navigation to routes the browser
			// hadn't already fetched.
			w.Header().Set("Cache-Control", "no-cache")
			r.URL.Path = "_shell.html"
		}
		handler.ServeHTTP(w, r)
	})
}
