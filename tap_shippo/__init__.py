#!/usr/bin/env python3

import os
import re
import time

import backoff
import requests
import singer
import strict_rfc3339
from singer import utils


REQUIRED_CONFIG_KEYS = ['start_date', 'token']
BASE_URL = "https://api.goshippo.com/"
URL_PATTERN = r'https://api.goshippo.com/(\w+).*'
CONFIG = {}
SESSION = requests.Session()
LOGGER = singer.get_logger()

# Field names, for the results we get from Shippo, and for the state map
LAST_START_DATE = 'last_start_date'
THIS_START_DATE = 'this_start_date'
OBJECT_UPDATED = 'object_updated'
START_DATE = 'start_date'
NEXT = 'next'

# List of all the endpoints we'll sync.
ENDPOINTS = [
    BASE_URL + "addresses?results=10",
    BASE_URL + "parcels?results=10",
    BASE_URL + "shipments?results=10",
    BASE_URL + "transactions?results=10",
    BASE_URL + "refunds?results=10"
]


def load_schema(stream):
    '''Returns the schema for the specified stream'''
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        "schemas/{}.json".format(stream))
    return utils.load_json(path)


def client_error(exc):
    '''Indicates whether the given RequestException is a 4xx response'''
    return exc.response is not None and 400 <= exc.response.status_code < 500


def parse_stream_from_url(url):
    '''Given a Shippo URL, extract the stream name (e.g. "addresses")'''
    match = re.match(URL_PATTERN, url)
    if not match:
        raise ValueError("Can't determine stream from URL " + url)
    return match.group(1)


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=client_error,
                      factor=2)
def request(url):
    '''Make a request to the given Shippo URL.

    Handles retrying, status checking. Logs request duration and records
    per second

    '''
    headers = {'Authorization': 'ShippoToken ' + CONFIG['token']}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']
    req = requests.Request("GET", url, headers=headers).prepare()
    LOGGER.info("GET %s", req.url)
    start_time = time.time()
    resp = SESSION.send(req)
    resp.raise_for_status()
    duration = time.time() - start_time
    data = resp.json()
    size = len(data['results'])
    LOGGER.info("Got %d records in %.0f seconds, %.2f r/s",
                size, duration, size / duration)
    return data

def sync_endpoint(url, state):

    stream = parse_stream_from_url(url)
    LOGGER.info("Replicating all %s from %s", stream, start)

    yield singer.SchemaMessage(
        stream=stream,
        schema=load_schema(stream),
        key_properties=["object_id"])

    rows_read = 0
    rows_written = 0
    while url:
        state[NEXT] = url
        yield singer.StateMessage(value=state)
        data = request(url)

        for row in data['results']:
            rows_read += 1
            if row[OBJECT_UPDATED] >= state[LAST_START_DATE]:
                yield singer.RecordMessage(stream=stream, record=row)
                rows_written += 1

        url = data.get(NEXT)

    if rows_read:
        LOGGER.info("Done syncing %s. Read %d records, wrote %d (%.2f%%)",
                    stream, rows_read, rows_written, 100.0 * rows_written / float(rows_read))


def get_starting_urls(state):
    next_url = state.get(NEXT)
    if next_url is None:
        return ENDPOINTS
    else:
        urls = []
        target_stream = parse_stream_from_url(next_url)
        LOGGER.info('Will pick up where we left off with URL %s (stream %s)',
                    next_url, target_stream)
        for url in ENDPOINTS:
            if parse_stream_from_url(url) == target_stream:
                urls.append(next_url)
            elif len(urls) > 0:
                urls.append(url)
        if len(urls) == 0:
            raise Exception('Unknown stream ' + target_stream)
        return urls


def do_sync(state):
    LOGGER.info("Starting sync")
    urls = get_starting_urls(state)
    LOGGER.info('I will sync urls in this order: %s', urls)
    for url in urls:
        for msg in sync_endpoint(url, state):
            singer.write_message(msg)
    state[NEXT] = None
    state[LAST_START_DATE] = state[THIS_START_DATE]
    state[THIS_START_DATE] = None
    singer.write_state(state)
    LOGGER.info("Sync completed")


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    state = copy.deepcopy(args.state)
    if LAST_START_DATE not in state:
        state[LAST_START_DATE] = CONFIG[START_DATE]
    if THIS_START_DATE not in state:
        state[THIS_START_DATE] = strict_rfc3339.now_to_rfc3339_utcoffset()
    do_sync(state)


if __name__ == '__main__':
    main()
