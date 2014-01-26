#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import urlnorm 

PLAYLIST_RE = r"(?im)^(file(\d+)=)?(http(.*?))$"

def normalize_url(url, path=None):
    try:
        if path:
            url = urljoin(url, path)
        url = urlnorm.norm(url)
        # force HTTP protocol
        if url.startswith('http'):
            return url
    except urlnorm.InvalidUrl:
        pass

def parse_playlist(text):
    urls = set([normalize_url(match.group(3).strip()) for match in re.finditer(PLAYLIST_RE, text)])
    return filter(None, urls)