#!/usr/bin/env python3

import backoff
import os
import time

import requests
import singer

from singer import utils


REQUIRED_CONFIG_KEYS = ['start_date', 'token']
BASE_URL = "https://api.goshippo.com/"
STATE = {}
CONFIG = {}

session = requests.Session()
logger = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


def client_error(e):
    return e.response is not None and 400 <= e.response.status_code < 500

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=client_error,
                      factor=2)
def gen_request(endpoint):
    if endpoint in STATE and STATE[endpoint].startswith(BASE_URL):
        url = STATE[endpoint]
    else:
        url = BASE_URL + endpoint + "?results=1000"

    headers = {'Authorization': 'ShippoToken ' + CONFIG['token']}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    while url:
        req = requests.Request("GET", url, headers=headers).prepare()
        logger.info("GET {}".format(req.url))
        start_time = time.time()
        resp = session.send(req)
        duration = time.time() - start_time
        resp.raise_for_status()
        data = resp.json()
        rows = data['results']
        url = data.get('next')
        STATE[endpoint] = url
        logger.info("Got %d records in %.0f seconds, %.2f r/s", len(rows), duration, len(rows) / duration)
        for row in rows:
            yield row


def sync_entity(entity):
    start = get_start(entity)
    logger.info("Replicating all {} from {}".format(entity, start))

    schema = load_schema(entity)
    singer.write_schema(entity, schema, ["object_id"])

    for row in gen_request(entity):
        if row['object_updated'] >= start:
            singer.write_record(entity, row)
            utils.update_state(STATE, entity, row['object_updated'])

    # Note that we don't print out state until the end
    singer.write_state(STATE)


def do_sync():
    logger.info("Starting sync")

    sync_entity("addresses")
    sync_entity("parcels")
    sync_entity("shipments")
    sync_entity("transactions")
    sync_entity("refunds")

    logger.info("Sync completed")


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    STATE.update(args.state)
    do_sync()


if __name__ == '__main__':
    main()
