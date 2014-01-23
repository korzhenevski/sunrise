package radio

import (
	"fmt"
	"github.com/outself/sunrise/http2"
	"regexp"
	"strings"
)

var StreamTitleRe = regexp.MustCompile("StreamTitle='(.*?)'")

type StreamInfo struct {
	Name        string `bson:"name"`
	Url         string `bson:"url"`
	Genre       string `bson:"genre"`
	ContentType string `bson:"content_type"`
	Private     bool   `bson:"private"`
	Bitrate     int    `bson:"bitrate"`
	Metaint     int    `bson:"metaint"`
}

func ExtractInfo(header *http2.Header) (info *StreamInfo) {
	info = &StreamInfo{}
	info.Name = strings.TrimSpace(header.Get("Icy-Name"))
	info.Url = strings.TrimSpace(header.Get("Icy-Url"))
	info.Genre = strings.TrimSpace(header.Get("Icy-Genre"))
	info.ContentType = strings.TrimSpace(header.Get("Content-Type"))

	// any value except zero mark stream is public
	if header.Get("Icy-Pub") == "0" {
		info.Private = true
	}

	fmt.Sscanf(header.Get("Icy-Br"), "%d", &info.Bitrate)
	fmt.Sscanf(header.Get("Icy-Metaint"), "%d", &info.Metaint)
	return
}

func ExtractTitle(meta string) string {
	match := StreamTitleRe.FindStringSubmatch(meta)
	if len(match) == 2 {
		return strings.TrimSpace(match[1])
	}
	return ""
}
