#!/usr/bin/python

import gevent.monkey

from datetime import datetime, timedelta
from memoize import Memoizer
from re import compile
from time import strftime, gmtime
from umsgpack import unpackb, UnpackException
from urlparse import parse_qsl

import logging

import config
import greenlog

ELASTIC_INDEX = 'logstash-m1-%Y.%m.%d'
NGINX_KEYS = ('time_local', 'remote_addr', 'server_protocol', 'request_method',
              'http_host', 'request', 'status', 'body_bytes_sent',
              'request_time', 'upstream_response_time', 'upstream_addr',
              'upstream_status', 'http_referer', 'http_x_forwarded_for',
              'http_user_agent')
NGINX_ACCESSLOG_KEYS = frozenset(
    ('@timestamp', '@version', 'host', 'type', 'remote_addr', 'server_protocol',
     'request_method', 'http_host', 'request', 'status', 'body_bytes_sent',
     'request_time', 'api_key', 'upstream_response_time', 'upstream_addr',
     'upstream_status', 'http_referer', 'http_x_forwarded_for',
     'http_user_agent', 'tags'))
NGINX_ERRORLOG_KEYS = frozenset(
    ('@timestamp', '@version', 'type', 'host', 'severity', 'error', 'errmsg'))
RE_APIKEY = compile('key=(?P<api_key>\w+)')
RE_NGINX_ERRORLOG = compile('^(?P<time_local>\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d)\
 \[(?P<severity>debug|info|notice|warn|error|crit|alert|emerg)\] (?P<pid>\d+)# \
 \d+: \*\d+ (?P<errmsg>.+)$')

cache_store = {}
memo = Memoizer(cache_store)


@memo(max_age=60)
def _extract_accesslog_time(timestr):
    try:
        timestr, tz = timestr.split(' ', 1)
        timestamp = datetime.strptime(timestr, '%d/%b/%Y:%H:%M:%S')
        offset = (int(tz[-4:-2]) * 60 + int(tz[-2:])) * 60
        if tz[0] == '-':
            offset = -offset
        timestamp = timestamp - timedelta(seconds=offset)
        return timestamp.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    except StandardError:
        return strftime('%Y-%m-%dT%H:%M:%S.000Z', gmtime())


@memo(max_age=60)
def _extract_errorlog_time(timestr):
    try:
        timestamp = datetime.strptime(timestr, '%Y/%m/%d %H:%M:%S')
        return timestamp.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    except StandardError:
        return strftime('%Y-%m-%dT%H:%M:%S.000Z', gmtime())


def _mangle_type_nginx_errorlog(event):
    if 'message' in event:
        m = RE_NGINX_ERRORLOG.match(event['message'])
        if m is not None:
            event.update(m.groupdict())
    if 'request' in event:
        m = RE_APIKEY.search(event['search'])
        if m is not None:
            event.update(m.groupdict())
    if 'time_local' in event:
        event['@timestamp'] = _extract_errorlog_time(event['time_local'])
    event = {k: v for k, v in event.iteritems() if k in NGINX_ERRORLOG_KEYS}
    return event


def _mangle_type_nginx_accesslog(event):
    if 'message' in event:
        chunks = event['message'].split('|', len(NGINX_KEYS) - 1)
        event.update(dict(zip(NGINX_KEYS, chunks)))
    if 'time_local' in event:
        event['@timestamp'] = _extract_accesslog_time(event['time_local'])
    for key in ('request_time', 'upstream_response_time'):
        try:
            event[key] = float(event[key])
        except KeyError:
            pass
        except ValueError:
            del event[key]
    for key in ('status', 'body_bytes_sent', 'upstream_status'):
        try:
            event[key] = int(event[key])
        except KeyError:
            pass
        except ValueError:
            del event[key]
    if 'request' in event:
        qs = event['request'].partition('?')[-1]
        get_parameters = {'http_get_' + k: v for k, v in parse_qsl(qs)}
        event.update(get_parameters)
    if 'http_get_key' in event:
        event['api_key'] = event['http_get_key']
    event = {k: v for k, v in event.iteritems() if (k in NGINX_ACCESSLOG_KEYS)
             or k.startswith('http_get_')}
    return event


def _mangle_type_unknown(event):
    return event


def _mangle_events(events):
    newevents = []
    for event in events:
        if event['type'] == 'nginx_accesslog':
            event = _mangle_type_nginx_accesslog(event)
        elif event['type'] == 'nginx_errorlog':
            event = _mangle_type_nginx_errorlog(event)
        else:
            event = _mangle_type_unknown(event)
        newevents.append(event)
    return newevents


def _decode_events(raw_events):
    events = []
    for raw_event in raw_events:
        try:
            event = unpackb(raw_event)
            events.append(event)
        except UnpackException:
            logging.error('Error unpacking message: {!r}'.format(raw_event))
    return events


class FilterCustom(greenlog.BaseGreenlet):
    def __init__(self):
        super(FilterCustom, self).__init__()

    def _run(self):
        while True:
            events = self.get_events()
            events = _decode_events(events)
            events = _mangle_events(events)
            self.put_events(events)


def main():
    gevent.monkey.patch_socket()
    in_redis = greenlog.InputRedis('logs', redis_config=config.redis)
    filter_custom = FilterCustom()
    out_es = greenlog.OutputElastic(ELASTIC_INDEX,
                                    es_config=[
                                        {'host': '10.77.255.17', 'port': 9200}])
    in_redis.to(filter_custom)
    filter_custom.to(out_es)
    greenlog.startall()
    greenlog.joinall()


if __name__ == '__main__':
    main()
