package manager

import (
	"fmt"
	"github.com/outself/sunrise/http2"
	"github.com/reusee/mmh3"
	neturl "net/url"
	"regexp"
	"strings"
)

var (
	StreamTitleRe = regexp.MustCompile("StreamTitle='(.*?)'")
)

type SInfo struct {
	Name        string
	Url         string
	Genre       string
	ContentType string
	Private     bool
	Bitrate     int
	Metaint     int
}

func FastHash(meta string) uint32 {
	return mmh3.Hash32([]byte(meta))
}

func ExtractStreamTitle(meta string) string {
	match := StreamTitleRe.FindStringSubmatch(meta)
	if len(match) == 2 {
		return strings.TrimSpace(match[1])
	}
	return ""
}

func NormalizeUrl(rawurl string) (string, error) {
	url, err := neturl.Parse(strings.TrimSpace(rawurl))
	if err != nil {
		return "", nil
	}
	return url.String(), nil
}

func ExtractStreamInfo(header *http2.Header) (info SInfo) {
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
