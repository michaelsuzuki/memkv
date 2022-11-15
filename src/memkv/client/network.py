import socket


class MessageClient(object):
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def read(self, size: int) -> bytes:
        bytes_to_read = size
        read_bytes = b""
        while bytes_to_read > 0:
            latest_bytes = self.socket.recv(bytes_to_read)
            bytes_to_read -= len(latest_bytes)
            read_bytes += latest_bytes
        return read_bytes

    def write(self)