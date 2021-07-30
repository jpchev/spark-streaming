import urllib.request
import socket
import time

url = "https://data.ffvl.fr/json/relevesmeteo.json"
HOST, PORT = "localhost", 20000

import socketserver

def fetchData():
    with urllib.request.urlopen(url) as response:
        return response.read()

class Handler_TCPServer(socketserver.BaseRequestHandler):
    def handle(self):
        print("handle request")
        # self.request - TCP socket connected to the client
        while(True):
            self.request.send(fetchData())
            time.sleep(5)

if __name__ == "__main__":
    tcp_server = socketserver.TCPServer((HOST, PORT), Handler_TCPServer)

    # Activate the TCP server.
    # To abort the TCP server, press Ctrl-C.
    tcp_server.serve_forever()
