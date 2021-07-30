from http.server import BaseHTTPRequestHandler, HTTPServer
from pymemcache.client.base import PooledClient
from pymemcache import serde
import json

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        dataDict = memcached_client.get("bitcoin_stats")
        dataArray =  list(map(lambda x: {"date": x[0], "min": x[1]["min"]["bpi"]["EUR"]["rate_float"], "ts_min": x[1]["min"]["time"]["updatedISO"], "ts_max": x[1]["max"]["time"]["updatedISO"], "max": x[1]["max"]["bpi"]["EUR"]["rate_float"]}, dataDict.items()))
        message = json.dumps(dataArray)
        self.wfile.write(bytes(message, "utf8"))

memcached_client = PooledClient('127.0.0.1', max_pool_size=4, serde=serde.pickle_serde)

with HTTPServer(('', 8000), handler) as server:
    server.serve_forever()
