#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import urlnorm 
from flask import Response
from werkzeug.urls import uri_to_iri

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

class RedirectResponse(Response):
    default_status = 302
    autocorrect_location_header = False

    def get_wsgi_headers(self, environ):
        headers = Response.get_wsgi_headers(self, environ)
        headers['Location'] = headers['Location'].replace('%3Ccomma%3E', ';')
        return headers

def raw_redirect(location):
    location = location.replace(';', '<comma>')
    response = RedirectResponse()
    response.headers['Location'] = location.encode('utf-8').decode()
    return response