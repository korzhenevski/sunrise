package manager

import (
	"github.com/reusee/mmh3"
	neturl "net/url"
	"regexp"
	"strings"
)

var (
	StreamTitleRe = regexp.MustCompile("StreamTitle='(.*?)'")
)

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
