# tap-shippo

A [Singer](https://singer.io) tap for extracting data from the Shippo
API.

## Limitations

Because the Shippo API does not allow for filtering records by date or ID, all runs
of the Shippo tap will pull all data. 

# Supported Endpoints

The following objects are pulled from the Shippo API:
* Addresses
* Parcels
* Refunds
* Shipments
* Transactions

This implementation does not retrieve customs items, customs declarations, manifests,
or carrier accounts. 

## Install

Clone this repository, and then:

```bash
› python setup.py install
```

## Run

#### Run the application

```bash

tap-shippo -c config.json -s state.json

```

Where `config.json` contains the following, retrieved from the API
section of your Shippo account settings page:

```json
{
  "token": "longalphanumericstring"
}
```

and `state.json` is a file containing only the value of the last state
message.

---

Copyright &copy; 2017 Stitch
