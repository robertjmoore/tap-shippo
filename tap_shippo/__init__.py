#!/usr/bin/env python3

import backoff

import requests
import singer

from tap_shippo import utils


REQUIRED_CONFIG_KEYS = ['start_date', 'token']
BASE_URL = "https://api.goshippo.com/"
STATE = {}
CONFIG = {}

session = requests.Session()
logger = singer.get_logger()


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
    url = BASE_URL + endpoint
    headers = {'Authorization': 'ShippoToken ' + CONFIG['token']}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    while True:
        req = requests.Request("GET", url, headers=headers).prepare()
        logger.debug("GET {}".format(req.url))
        resp = session.send(req)
        resp.raise_for_status()

        data = resp.json()
        for row in data['results']:
            yield row

        if data['next'] is None:
            break

        url = data['next']


def sync_entity(entity):
    start = get_start(entity)
    logger.info("Replicating all {} from {}".format(entity, start))

    schema = utils.load_schema(entity)
    singer.write_schema(entity, schema, ["object_id"])

    for row in gen_request(entity):
        if row['object_updated'] >= start:
            singer.write_record(entity, row)
            utils.update_state(STATE, entity, row['object_updated'])

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
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    do_sync()


if __name__ == '__main__':
    main()
