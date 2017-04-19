#!/usr/bin/env python3

import os
import re
import time

import backoff
import requests
import singer

from singer import utils


REQUIRED_CONFIG_KEYS = ['start_date', 'token']
BASE_URL = "https://api.goshippo.com/"
URL_PATTERN = r'https://api.goshippo.com/(\w+).*'
CONFIG = {}
SESSION = requests.Session()
LOGGER = singer.get_logger()

# Field names, for the results we get from Shippo, and for the state map
MAX_OBJECT_UPDATED = 'max_object_updated'
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


def load_schema(entity):
    '''Returns the schema for the specified entity'''
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        "schemas/{}.json".format(entity))
    return utils.load_json(path)


def client_error(exc):
    '''Indicates whether the given RequestException is a 4xx response'''
    return exc.response is not None and 400 <= exc.response.status_code < 500

def parse_entity_from_url(url):
    '''Given a Shippo URL, extract the entity type (e.g. "addresses")'''
    match = re.match(URL_PATTERN, url)
    if not match:
        raise ValueError("Can't determine entity type from URL " + url)
    return match.group(1)


def init_state_for_entity(state, entity):
    '''Initialize the state for the given entity type. Ensures the
    max_object_updated field is set to the start date from the config if it
    doesn't already exist.

    '''
    if MAX_OBJECT_UPDATED not in state:
        state[MAX_OBJECT_UPDATED] = {}
    if entity not in state[MAX_OBJECT_UPDATED]:
        state[MAX_OBJECT_UPDATED][entity] = CONFIG[START_DATE]

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

    entity = parse_entity_from_url(url)
    init_state_for_entity(state, entity)
    start = state[MAX_OBJECT_UPDATED][entity]
    max_object_updated = start
    LOGGER.info("Replicating all %s from %s", entity, start)
    singer.write_schema(entity, load_schema(entity), ["object_id"])

    rows_read = 0
    rows_written = 0
    while url:
        state[NEXT] = url
        singer.write_state(state)
        data = request(url)

        for row in data['results']:
            updated = row[OBJECT_UPDATED]
            rows_read += 1
            if updated >= start:
                singer.write_record(entity, row)
                rows_written += 1
            if updated >= max_object_updated:
                max_object_updated = updated

        url = data.get(NEXT)

    if rows_read:
        LOGGER.info("Done syncing %s. Read %d records, wrote %d (%.2f%%)",
                    entity, rows_read, rows_written, 100.0 * rows_written / float(rows_read))
    # We don't update the state with the max observed object_updated until
    # we've gotten the whole batch, because the results are not in sorted
    # order.
    if max_object_updated > state[MAX_OBJECT_UPDATED][entity]:
        state[MAX_OBJECT_UPDATED][entity] = max_object_updated
    singer.write_state(state)

def get_starting_urls(state):
    next_url = state.get(NEXT)
    if next_url is None:
        return ENDPOINTS
    else:
        target_type = parse_entity_from_url(next_url)
        LOGGER.info('Will pick up where we left off with URL %s (entity type %s)',
                    next_url, target_type)
        pivot = None
        for i, url in enumerate(ENDPOINTS):
            if parse_entity_from_url(url) == target_type:
                pivot = i
        if pivot is None:
            raise Exception('Unknown entity type ' + target_type)
        return [next_url] + ENDPOINTS[pivot+1:] + ENDPOINTS[:max(pivot, 0)]


def do_sync(state):
    LOGGER.info("Starting sync")
    urls = get_starting_urls(state)
    LOGGER.info('I will sync urls in this order: %s', urls)
    for url in urls:
        sync_endpoint(url, state)
    state[NEXT] = None
    singer.write_state(state)
    LOGGER.info("Sync completed")


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    do_sync(args.state)


if __name__ == '__main__':
    main()
