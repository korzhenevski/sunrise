package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/PuerkitoBio/purell"
	"github.com/outself/sunrise/radio"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type PlaylistInfo struct {
	Url   string
	Error string
	Info  *radio.StreamInfo
}

var PlaylistUrlRegex = regexp.MustCompile(`(?im)^(:?file\d+=)?(http://.*)$`)

func uniqUrls(urls []string) (out []string) {
	set := make(map[string]struct{})

	for _, url := range urls {
		set[url] = struct{}{}
	}

	for url, _ := range set {
		out = append(out, url)
	}

	return
}

func parsePlaylistUrls(content string) []string {
	var urls []string
	normFlags := purell.FlagsSafe | purell.FlagRemoveUnnecessaryHostDots | purell.FlagRemoveFragment

	var streamUrl string
	result := PlaylistUrlRegex.FindAllStringSubmatch(content, -1)
	log.Printf("matches: %+v", result)

	for _, m := range result {
		streamUrl = strings.TrimSpace(m[2])
		if streamUrl, err := purell.NormalizeURLString(streamUrl, normFlags); err == nil {
			urls = append(urls, strings.TrimSpace(streamUrl))
		} else {
			log.Printf("parse %s error: %s", streamUrl, err)
		}
	}

	return urls
}

func fetchStreamInfo(url string) (*radio.StreamInfo, error) {
	r, err := radio.NewRadioWithTimeout(url, "WinampMPEG/5.0", 5*time.Second)

	if err != nil {
		log.Printf("failed %s: %s", url, err)
		return nil, err
	}

	defer r.Close()

	return radio.ExtractInfo(r.Header()), nil
}

func fetchStreams(urls []string, totalTimeout time.Duration) (results []*PlaylistInfo) {
	c := make(chan *PlaylistInfo)

	for _, url := range urls {
		go func(url string) {
			info, err := fetchStreamInfo(url)
			if err != nil {
				c <- &PlaylistInfo{Url: url, Error: fmt.Sprintf("%s", err)}
			} else {
				c <- &PlaylistInfo{Url: url, Info: info}
			}
		}(url)
	}

	timeout := time.After(totalTimeout)
	for i := 0; i < len(urls); i++ {
		select {
		case result := <-c:
			results = append(results, result)
		case <-timeout:
			fmt.Println("timed out")
			return
		}
	}

	return
}

func fetchPlaylist(url string) ([]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.New("fetch_playlist: not success response")
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if len(body) >= 1024*10 {
		return nil, errors.New("fetch_playlist: body is too large")
	}

	urls := parsePlaylistUrls(string(body))
	return urls, nil
}

func checkHandler(w http.ResponseWriter, r *http.Request) {
	url := r.FormValue("url")
	urls, err := fetchPlaylist(url)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	urls = uniqUrls(urls)

	log.Printf("urls: %+v", urls)
	results := fetchStreams(urls, 5*time.Second)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, Response{"results": results, "count": len(results)})
}

type Response map[string]interface{}

func (r Response) String() (s string) {
	b, err := json.Marshal(r)
	if err != nil {
		s = ""
		return
	}
	s = string(b)
	return
}

func main() {
	http.HandleFunc("/check", checkHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
