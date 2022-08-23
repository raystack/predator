package v1beta1

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func getRequestBody(r *http.Request, body interface{}) error {
	decoder := json.NewDecoder(r.Body)
	return decoder.Decode(&body)
}

func printError(w http.ResponseWriter, err error, code int) {
	w.WriteHeader(code)

	fmt.Fprint(w, err)
}
