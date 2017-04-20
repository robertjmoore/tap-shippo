import unittest
from tap_shippo import *

class TestParseEntityFromUrl(unittest.TestCase):

    def test_succeeds(self):
        url = "https://api.goshippo.com/addresses?results=1000&page=2"
        entity = parse_entity_from_url(url)
        self.assertEqual(entity, "addresses")

    def test_fails(self):
        with self.assertRaises(ValueError):
            entity = parse_entity_from_url("foobar")

    
class TestGetStartingUrls(unittest.TestCase):

    def test_with_empty_state(self):
        urls = get_starting_urls({})
        self.assertEqual(urls, ENDPOINTS)

    def test_with_none(self):
        urls = get_starting_urls({'next': None})
        self.assertEqual(urls, ENDPOINTS)        

    def test_with_first(self):
        url = 'https://api.goshippo.com/addresses?results=1000&page=2'
        urls = get_starting_urls({'next': url})
        self.assertEqual(urls, [
            url,
            ENDPOINTS[1],
            ENDPOINTS[2],
            ENDPOINTS[3],
            ENDPOINTS[4]])

    def test_with_last(self):
        url = 'https://api.goshippo.com/refunds?results=1000&page=2'
        urls = get_starting_urls({'next': url})
        LOGGER.info('Urls are %s', urls)
        self.assertEqual(urls, [url])

    def test_with_middle(self):
        url = 'https://api.goshippo.com/shipments?results=1000&page=2'
        urls = get_starting_urls({'next': url})
        self.assertEqual(urls, [
            url,
            ENDPOINTS[3],
            ENDPOINTS[4]])
