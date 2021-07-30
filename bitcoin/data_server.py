import urllib.request
import socket

url = "https://api.coindesk.com/v1/bpi/currentprice.json"
HOST, PORT = "localhost", 9999

import socketserver

def fetchData():
    with urllib.request.urlopen(url) as response:
        return response.read()

class Handler_TCPServer(socketserver.BaseRequestHandler):
    def handle(self):
        # self.request - TCP socket connected to the client
        self.request.send(fetchData())

if __name__ == "__main__":
    tcp_server = socketserver.TCPServer((HOST, PORT), Handler_TCPServer)

    # Activate the TCP server.
    # To abort the TCP server, press Ctrl-C.
    tcp_server.serve_forever()
