package main

import (
	"fmt"
	"io/ioutil"
	"io"
	"log"
	"net/http"
	"compress/gzip"
)

func logreceiver(w http.ResponseWriter, r *http.Request) {
	log.Printf("::: Starting request")
	log.Printf("Method: %s", r.Method)
	if r.Method!="POST" {
		http.Error(w, "Wrong method", http.StatusBadRequest)
		return
	}
	if r.Header.Get("Content-Type")!="application/json" {
		http.Error(w, "Invalid content type", http.StatusBadRequest)
		return
	}
	for header, v := range r.Header {
		for _, i := range v {
			log.Printf("Header: %s: %s", header, i)
		}
	}
	var reader io.ReadCloser
	var err error
	switch r.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(r.Body)
		if err!=nil {
			log.Printf("Error creating gzip reader: %s", err.Error())
			http.Error(w, "Error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer reader.Close()
	default:
	reader = r.Body
	}
	body, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Printf("Error reading body: %s\n", err.Error())
		http.Error(w, "Error: "+err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("Body: %s", string(body))
	fmt.Fprintf(w, "OK")
}


func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("Starting log receiver")
	http.HandleFunc("/log", logreceiver)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Printf("Fatal error: %s", err.Error())
	}
}
