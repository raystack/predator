package handler

import (
	"encoding/json"
	"log"
	"net/http"
)

//Ping is a http handler for ping endpoint
func Ping() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode("pong")
		if err != nil {
			log.Println(err)
		}
	}
}
