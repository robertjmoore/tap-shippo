#!/usr/bin/env python3

import os
import argparse
import backoff
import logging
import requests
import singer
import sys
import json
import datetime

session = requests.Session()
logger = singer.get_logger()
state = {}


def client_error(e):
    return e.response is not None and 400 <= e.response.status_code < 500

@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=client_error,
                      factor=2)
def authed_get(url):
    resp = session.request(method='get', url=url)
    resp.raise_for_status()
    return resp

def authed_get_all_pages(url):
    while True:
        r = authed_get(url)
        rJson = r.json();
        yield rJson['results']
        if (rJson['next'] is None):
            break;
        url = rJson['next']

address_schema = {'type': 'object',
                 'properties': {
                     'object_state': {
                         'type': 'string',
                     },
                     'object_purpose': {
                         'type': 'string',
                     },
                     'object_source': {
                         'type': 'string',
                     },
                     'object_created': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_updated': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_id': {
                         'type': 'string'
                     },
                     'object_owner': {
                         'type': 'string'
                     },
                     'name': {
                         'type': 'string'
                     },
                     'company': {
                         'type': 'string'
                     },
                     'street1': {
                         'type': 'string'
                     },
                     'street2': {
                         'type': 'string'
                     },
                     'city': {
                         'type': 'string'
                     },
                     'state': {
                         'type': 'string'
                     },
                     'zip': {
                         'type': 'string'
                     },
                     'country': {
                         'type': 'string'
                     },
                     'phone': {
                         'type': 'string'
                     },
                     'email': {
                         'type': 'string'
                     },
                     'is_residential': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "boolean",
                             }
                         ]
                     },
                     'metadata': {
                         'type': 'string'
                     },
                     'test': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "boolean",
                             }
                         ]
                     },
                     'messages': {
                         'type': 'array',
                         'items': {
                             'code': {
                                 'type': 'string',
                             },
                             'source': {
                                 'type': 'string',
                             },
                             'text': {
                                 'type': 'string',
                             },
                         }
                     }
                 },
                 'required': ['object_id']
             }

parcel_schema = {'type': 'object',
                 'properties': {
                     'object_state': {
                         'type': 'string',
                     },
                     'object_created': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_updated': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_id': {
                         'type': 'string'
                     },
                     'object_owner': {
                         'type': 'string'
                     },
                     'template': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "string",
                             }
                         ]
                     },
                     'length': {
                         'type': 'string'
                     },
                     'width': {
                         'type': 'string'
                     },
                     'height': {
                         'type': 'string'
                     },
                     'distance_unit': {
                         'type': 'string'
                     },
                     'weight': {
                         'type': 'string'
                     },
                     'mass_unit': {
                         'type': 'string'
                     },
                     'metadata': {
                         'type': 'string'
                     },
                     'extra': {
                         'type': 'object',
                         'properties': {
                         }
                     },
                     'test': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "boolean",
                             }
                         ]
                     },
                 },
                 'required': ['object_id']
             }

shipment_schema = {'type': 'object',
                 'properties': {
                     'object_state': {
                         'type': 'string',
                     },
                     'object_status': {
                         'type': 'string',
                     },
                     'object_purpose': {
                         'type': 'string',
                     },
                     'object_created': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_updated': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_id': {
                         'type': 'string'
                     },
                     'object_owner': {
                         'type': 'string'
                     },
                     'object_from': {
                         'type': 'string'
                     },
                     'object_to': {
                         'type': 'string'
                     },
                     'object_return': {
                         'type': 'string'
                     },
                     'object_parcel': {
                         'type': 'string'
                     },
                     'submission_date': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'insurance_amount': {
                         'type': 'string'
                     },
                     'insurance_currency': {
                         'type': 'string'
                     },
                     'extra': {
                         'type': 'object'
                     },
                     'customs_declaration': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "string",
                             }
                         ]
                     },
                     'reference1': {
                         'type': 'string'
                     },
                     'reference2': {
                         'type': 'string'
                     },
                     'rates_url': {
                         'type': 'string'
                     },
                     'rates_list': {
                         'type': 'array'
                     },
                     'carrier_accounts': {
                         'type': 'array'
                     },
                     'metadata': {
                         'type': 'string'
                     },
                     'test': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "boolean",
                             }
                         ]
                     },
                     'messages': {
                         'type': 'array',
                         'items': {
                             'code': {
                                 'type': 'string',
                             },
                             'source': {
                                 'type': 'string',
                             },
                             'text': {
                                 'type': 'string',
                             },
                         }
                     }
                 },
                 'required': ['object_id']
             }

transaction_schema = {'type': 'object',
                 'properties': {
                     'object_state': {
                         'type': 'string',
                     },
                     'object_status': {
                         'type': 'string',
                     },
                     'object_created': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_updated': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_id': {
                         'type': 'string'
                     },
                     'object_owner': {
                         'type': 'string'
                     },
                     'test': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "boolean",
                             }
                         ]
                     },
                     'rate': {
                         'type': 'string'
                     },
                     'tracking_number': {
                         'type': 'string'
                     },
                     'tracking_status': {
                         'type': 'object'
                     },
                     'tracking_history': {
                         'type': 'array'
                     },
                     'tracking_url_provider': {
                         'type': 'string'
                     },
                     'label_url': {
                         'type': 'string'
                     },
                     'commercial_invoice_url': {
                         'type': 'string'
                     },
                     'metadata': {
                         'type': 'string'
                     },
                     'messages': {
                         'type': 'array',
                         'items': {
                             'code': {
                                 'type': 'string',
                             },
                             'source': {
                                 'type': 'string',
                             },
                             'text': {
                                 'type': 'string',
                             },
                         }
                     }
                 },
                 'required': ['object_id']
             }

refund_schema = {'type': 'object',
                 'properties': {
                     'object_status': {
                         'type': 'string',
                     },
                     'object_created': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_updated': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'object_id': {
                         'type': 'string'
                     },
                     'object_owner': {
                         'type': 'string'
                     },
                     'transaction': {
                         'type': 'string'
                     },
                     'test': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "boolean",
                             }
                         ]
                     },
                 },
                 'required': ['object_id']
             }

def write_all_records(url, schemaName):
    for results in authed_get_all_pages(url):
        singer.write_records(schemaName, results)

def do_sync(args):
    global state
    with open(args.config) as config_file:
        config = json.load(config_file)

    missing_keys = []
    for key in ['token']:
        if key not in config:
            missing_keys += [key]

    if len(missing_keys) > 0:
        logger.fatal("Missing required configuration keys: {}".format(missing_keys))

    session.headers.update({'authorization': 'ShippoToken ' + config['token']})

    if args.state:
        with open(args.state, 'r') as file:
            for line in file:
                state = json.loads(line.strip())

    logger.info('Replicating all addresses')
    singer.write_schema('addresses', address_schema, 'object_id')
    write_all_records('https://api.goshippo.com/addresses/', 'addresses')

    logger.info('Replicating all parcels')
    singer.write_schema('parcels', parcel_schema, 'object_id')
    write_all_records('https://api.goshippo.com/parcels/', 'parcels')

    logger.info('Replicating all shipments')
    singer.write_schema('shipments', shipment_schema, 'object_id')
    write_all_records('https://api.goshippo.com/shipments/', 'shipments')

    logger.info('Replicating all transactions')
    singer.write_schema('transactions', transaction_schema, 'object_id')
    write_all_records('https://api.goshippo.com/transactions/', 'transactions')

    logger.info('Replicating all refunds')
    singer.write_schema('refunds', refund_schema, 'object_id')
    write_all_records('https://api.goshippo.com/refunds/', 'refunds')

    #because there incremental replication is not possible, writing state is not actually
    #doing anything but sending back whatever state, if any, was passed in at the beginning
    singer.write_state(state)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    do_sync(args)

    
if __name__ == '__main__':
    main()
