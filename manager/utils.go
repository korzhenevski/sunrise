package manager

import (
	"github.com/reusee/mmh3"
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
