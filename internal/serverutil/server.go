package serverutil

import (
	"encoding/json"
	"log"
	"net/http"
)

func DecodeBody(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	if err := d.Decode(v); err != nil {
		return err
	}
	log.Printf("BODY %+v", v)
	return nil
}

func HttpError(w http.ResponseWriter, code int) {
	log.Println("ERROR", code)
	http.Error(w, http.StatusText(code), code)
}

func HttpJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	if err := e.Encode(v); err != nil {
		HttpError(w, http.StatusInternalServerError)
		return
	}
}
