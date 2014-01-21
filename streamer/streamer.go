package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

func SendBlob(w http.ResponseWriter, req *http.Request) {
	path := req.FormValue("path")
	if path == "" {
		http.Error(w, "no path given", http.StatusBadRequest)
		return
	}
	path = filepath.Clean(path)

	f, err := os.Open(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	d, err := f.Stat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	off, err := strconv.ParseInt(req.FormValue("offset"), 10, 0)
	if err != nil || off < 0 {
		http.Error(w, "no valid offset", http.StatusBadRequest)
		return
	}

	n, err := strconv.ParseInt(req.FormValue("size"), 10, 0)
	if err != nil {
		http.Error(w, "no valid length", http.StatusBadRequest)
		return
	}

	if n == 0 {
		n = d.Size()
	}

	content := io.NewSectionReader(f, off, n)
	name := filepath.Base(path)

	log.Printf("%s %d/%d", path, off, n)
	http.ServeContent(w, req, name, d.ModTime(), content)
}

func main() {
	http.HandleFunc("/sb", SendBlob)

	err := http.ListenAndServe(":12345", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
