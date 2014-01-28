package backend

import (
	"fmt"
	"github.com/PuerkitoBio/purell"
	"github.com/outself/sunrise/radio"
	"log"
	"regexp"
	"strings"
	"time"
)

type PlaylistInfo struct {
	Url   string
	Error string
	Info  *radio.StreamInfo
}

var playlistUrlRe = regexp.MustCompile(`(?im)^(:?file\d+=)?(http://.*)$`)
var urlNormFlags = purell.FlagsSafe | purell.FlagRemoveUnnecessaryHostDots | purell.FlagRemoveFragment

func normalizeUrl(url string) (string, error) {
	return purell.NormalizeURLString(url, urlNormFlags)
}

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

func parsePlaylist(content string) (urls []string) {
	var streamUrl string
	result := playlistUrlRe.FindAllStringSubmatch(content, -1)

	for _, m := range result {
		streamUrl = strings.TrimSpace(m[2])
		if streamUrl, err := normalizeUrl(streamUrl); err == nil {
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
