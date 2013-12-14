#!/usr/bin/env python
# -*- coding: utf-8 -*-

from app import app
from datetime import datetime, timedelta, date
import email.utils

import humanize

MONTH_NAMES = (u"янв", u"фев", u"мар", u"апр", u"май", u"июн", u"июл", u"авг", u"сен", u"окт", u"ноя", u"дек")

@app.template_filter()
def humantime(dt):
    if isinstance(dt, int):
        dt = datetime.fromtimestamp(dt)

    delta = datetime.now() - dt
    days = abs(delta.days)
    years = days // 365
    daymap = {0: u'сегодня', 1: u'вчера', 2: u'позавчера'}
    month = MONTH_NAMES[dt.month - 1]

    if years == 0:
        if days == 0:
            day = u''
        elif days <= 2:
            day = daymap.get(days)
        else:
            day = u'%d %s' % (dt.day, month)
    else:
        day = u'%d %s %d' % (dt.day, month, dt.year)

    return unicode(day + u' в ' + dt.strftime(u"%H:%M"))


@app.template_filter()
def timeago(dt):
    return humanize.naturaltime(datetime.now() - dt)


@app.template_filter()
def intcomma(i):
    return humanize.intcomma(i)

@app.template_filter()
def to_duration(dur):
    dur = int(dur)
    if dur > 0:
        res = unicode(timedelta(seconds=dur))
        if dur <= 3600:
            return res[2:]
        return res
    return '00:00'


@app.template_filter()
def format_gmt(ts):
    return email.utils.formatdate(ts, usegmt=True)

# used in queue list
@app.template_filter()
def format_ts(ts, fmt=None):
    if not fmt:
        fmt = '%Y-%m-%d %H:%I:%S'
        return datetime.fromtimestamp(ts).strftime(fmt)


@app.template_filter()
def format_date(ts):
    return datetime.fromtimestamp(ts).strftime('%Y%m%d')


@app.template_filter()
def to_dt(ts):
    return datetime.fromtimestamp(ts)


@app.template_filter()
def queue_html_class(s):
    c = ''
    if s.get('task_id'):
        c = 'active'

    if s.get('record') and c:
        c = 'warning'

    if s.get('retry_ivl'):
        c = 'danger'

    return c