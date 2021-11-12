package main

import (
	"fmt"
	// 	"io/ioutil"
	"compress/gzip"
	"context"
	"encoding/json"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"io"
	"log"
	"net/http"
	"time"
)

type lambdaMsg struct {
	Date       float64 `json:"date"`
	Time       string  `json:"time"`
	Stream     string  `json:"stream"`
	P          string  `json:"_p"`
	Log        string  `json:"log"`
	Kubernetes struct {
		PodName        string            `json:"pod_name"`
		Namespace      string            `json:"namespace_name"`
		PodId          string            `json:"pod_id"`
		Labels         map[string]string `json:"labels"`
		Host           string            `json:"host"`
		ContainerName  string            `json"container_name"`
		DockerId       string            `json:"docker_id"`
		ContainerHash  string            `json:"container_hash"`
		ContainerImage string            `json:"container_image"`
	} `json:kubernetes`
}

var msg_chan chan lambdaMsg

func logreceiver(w http.ResponseWriter, r *http.Request) {
	log.Printf("::: Starting request")
	log.Printf("Method: %s", r.Method)
	if r.Method != "POST" {
		http.Error(w, "Wrong method", http.StatusBadRequest)
		return
	}
	if r.Header.Get("Content-Type") != "application/json" {
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
		if err != nil {
			log.Printf("Error creating gzip reader: %s", err.Error())
			http.Error(w, "Error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer reader.Close()
	default:
		reader = r.Body
	}
	var msg []lambdaMsg
	err = json.NewDecoder(reader).Decode(&msg)
	if err != nil {
		log.Printf("Error decoding msg: %s", err.Error())
		http.Error(w, "Error: "+err.Error(), http.StatusInternalServerError)
	}
	for _, m := range msg {
		msg_chan <- m
	}
	log.Printf("Message: %+v", msg)
	fmt.Fprintf(w, "OK")

}

const bufsize = 50
const buftime = 1000
func bg_writer(ctx context.Context, client immuclient.ImmuClient) {
	buffer := []lambdaMsg{}
	t := time.NewTimer(buftime* time.Millisecond)
	ticking:=true
	for {
		select {
		case msg := <-msg_chan:
			buffer = append(buffer, msg)
			if len(buffer) == bufsize {
				pushmsg(ctx, client, buffer)
				buffer = []lambdaMsg{}
				t.Stop()
				ticking=false
			} else if !ticking {
				t = time.NewTimer(buftime* time.Millisecond)
				ticking=true
			}
				
		case <-t.C:
			ticking=false
			if len(buffer) > 0 {
				pushmsg(ctx, client, buffer)
				buffer = []lambdaMsg{}
			}
		}
	}
}

func main() {
	cfg := immucfg{
		IpAddr:   "localhost",
		Port:     3322,
		Username: "immudb",
		Password: "immudb",
		DBName:   "defaultdb",
	}
	ctx := context.Background()
	ctx, client := connect(ctx, cfg)
	msg_chan = make(chan lambdaMsg, 10)
	go bg_writer(ctx, client)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("Starting log receiver")
	http.HandleFunc("/log", logreceiver)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Printf("Fatal error: %s", err.Error())
	}
}
