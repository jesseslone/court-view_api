package api

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var uiAssets embed.FS

func uiHandler() http.Handler {
	sub, err := fs.Sub(uiAssets, "static")
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		})
	}
	return http.FileServer(http.FS(sub))
}
