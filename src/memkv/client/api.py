import socket
import time
from typing import List
import memkv.protocol.memkv_pb2 as memkv_pb2
from memkv.protocol.util import construct_header_and_data

class Client(object):
    def __init__(self, host=str, port: int=9001):
        self.host = host
        self.port = port
        self.sd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected = False

    def connect(self):
        self.sd.connect(self.host, self.port)
    
    def send(self, data: bytes):
        retry = True
        while retry:
            try:
                while retry:
                    self.sd.sendall(data)
                    retry = False
            except Exception as e:
                time.sleep(.055)

    
    def get(self, keys: List[str]) -> List[bytes]:
        get = memkv_pb2.GetCommand(keys=keys)
        header, data = construct_header_and_data(get)
        self.send(header)
        self.send(data)
        header_bytes = self.sd.recv(6)
    def set(**kwargs) -> List[str]:
        return []

    def delete(keys: List[str]):
        pass