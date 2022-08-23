package handler

import (
	"encoding/json"
	"net/http"
)

//Ping is a http handler for ping endpoint
func Ping() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode("pong")
		return
	}
}
