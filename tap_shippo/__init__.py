#!/usr/bin/env python3

import os
import time

import backoff
import requests
import singer

from singer import utils


REQUIRED_CONFIG_KEYS = ['start_date', 'token']
BASE_URL = "https://api.goshippo.com/"
CONFIG = {}
SESSION = requests.Session()
LOGGER = singer.get_logger()

MAX_OBJECT_UPDATED = 'max_object_updated'
OBJECT_UPDATED = 'object_updated'
START_DATE = 'start_date'
NEXT = 'next'
ENDPOINTS = [
    BASE_URL + "addresses?results=1000",
    BASE_URL + "parcels?results=1000",
    BASE_URL + "shipments?results=1000",
    BASE_URL + "transactions?results=1000",
    BASE_URL + "refunds?results=1000"
]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def client_error(exc):
    return exc.response is not None and 400 <= exc.response.status_code < 500

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=client_error,
                      factor=2)

def parse_entity_from_url(url):
    return "addresses"


def init_state_for_entity(state, entity):
    if entity not in state[MAX_OBJECT_UPDATED]:
        state[MAX_OBJECT_UPDATED][entity] = CONFIG[START_DATE]


def request(url):
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
    start = state[entity][MAX_OBJECT_UPDATED]
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

    LOGGER.info("Done syncing %s. Read %d records, wrote %d (%.2f%%)",
                entity, rows_read, rows_written, 100.0 * rows_written / float(rows_read))
    # We don't update the state with the max observed object_updated until
    # we've gotten the whole batch, because the results are not in sorted
    # order.
    if max_object_updated > state[entity][OBJECT_UPDATED]:
        state[entity][OBJECT_UPDATED] = max_object_updated
    singer.write_state(state)

def get_starting_urls(state):
    next_url = state.get(NEXT)
    if next_url is None:
        return ENDPOINTS
    else:
        target_type = parse_entity_from_url(next_url)
        pivot = None
        for i, url in enumerate(ENDPOINTS):
            if parse_entity_from_url(url) == target_type:
                pivot = i
        if not pivot:
            raise Exception('Unknown entity type ' + target_type)
        endpoints = [next_url]
        if pivot == 0:
            endpoints.append(ENDPOINTS[1:])
        elif pivot == len(ENDPOINTS) - 1:
            endpoints.append(ENDPOINTS[:pivot])
        else:
            endpoints.append(ENDPOINTS[pivot+1:])
            endpoints.append(ENDPOINTS[:pivot-1])
        return endpoints


def do_sync(state):
    LOGGER.info("Starting sync")
    urls = get_starting_urls(state)
    LOGGER.info('I will sync urls in this order: %s', urls)
    for url in urls:
        sync_endpoint(url, state)
    state[NEXT] = None
    LOGGER.info("Sync completed")


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    do_sync(args.state)


if __name__ == '__main__':
    main()
