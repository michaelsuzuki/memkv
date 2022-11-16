import logging
import socket
from typing import Dict, List

import memkv.protocol.memkv_pb2 as memkv_pb2
from memkv.protocol.util import (
    HEADER_SIZE,
    MessageT,
    MessageWrapper,
    RetryableException,
    encode_into_header_and_data_bytes,
    construct_message,
    decode_header,
    with_backoff,
)


logger = logging.getLogger(__name__)


class ClientAPIException(Exception):
    pass


class Client(object):
    def __init__(self, host=str, port: int = 9001):
        self.host = host
        self.port = port
        self.sd = None
        self.connected = False

    def connect(self):
        if self.sd is None:
            self.sd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sd.connect(self.host, self.port)

    def send(self, data: bytes):
        try:
            self.sd.sendall(data)
        except socket.error as e:
            raise RetryableException(e)

    def receive(self, length: int) -> bytes:
        try:
            buffer: bytes = b""
            received_length = 0
            while received_length < length:
                buffer += self.sd.recv(length - len(buffer))
                received_length += len(buffer)
            return buffer
        except socket.error as e:
            logger.exception(e)
            self.close()
            raise RetryableException(e)

    def receive_response(self) -> memkv_pb2.Response:
        header_bytes = self.receive(HEADER_SIZE)
        header = decode_header(header_bytes)
        message_bytes = self.receive(header.message_size)
        wrapper = MessageWrapper(header=header, data=message_bytes)
        return construct_message(wrapper)

    @with_backoff(logger)
    def execute_command(self, msg: MessageT) -> memkv_pb2.Response:
        self.connect()
        header, data = encode_into_header_and_data_bytes(msg)
        self.send(header)
        self.send(data)
        return self.receive_response()

    def get(self, keys: List[str]) -> Dict[str, bytes]:
        get = memkv_pb2.GetCommand(keys=keys)
        response = self.execute_command(get)
        if response.status == "OK":
            return {kv.key: kv.value for kv in response.kv_list.key_values}
        else:
            raise ClientAPIException(f"Error processing GET request: {response.message}")

    def set(self, key_values: Dict[str, bytes]) -> List[str]:
        """Sets the keys to the values in **kwargs

        Note: values that are not bytes will cause exceptions and will cause this method to fail.

        key_values: key value dictionary where the keys are strings and the values are bytes.
        returns: List of keys that got updated.
        """
        key_values = [
            memkv_pb2.KeyValue(key=key, value=value) for key, value in key_values.items()
        ]
        set_cmd = memkv_pb2.SetCommand(key_values=key_values)
        response = self.execute_command(set_cmd)
        if response.status == "OK":
            return [key for key in response.key_list.keys]
        else:
            raise ClientAPIException(f"Error processing SET request: {response.message}")

    def delete(self, keys: List[str]) -> List[str]:
        """Attempts to delete provided keys from store

        keys: List of keys to delete
        returns: A list of keys that were deleted. Any keys not present did not exist to begin with.
        """
        delete_cmd = memkv_pb2.DeleteCommand(keys=keys)
        response = self.execute_command(delete_cmd)
        if response.status == "OK":
            return [key for key in response.key_list.keys]
        else:
            raise ClientAPIException(f"Error processing DELETE request: {response.message}")

    def metrics(
        self,
        get_key_count: bool = True,
        get_total_store_contents_size: bool = True,
        get_keys_read_count: bool = True,
        get_keys_updated_count: bool = True,
        get_keys_deleted_count: bool = True
    ) -> memkv_pb2.MetricsResponse:
        metrics_cmd = memkv_pb2.MetricsCommand(
            get_key_count=get_key_count,
            get_total_store_contents_size=get_total_store_contents_size,
            get_keys_read_count=get_keys_read_count,
            get_keys_updated_count=get_keys_updated_count,
            get_keys_deleted_count=get_keys_deleted_count,
        )
        response = self.execute_command(metrics_cmd)
        if response.status != "OK":
            raise ClientAPIException(f"Error executing the metrics request: {response.message}")
        return response.metrics

    def close(self):
        try:
            if self.sd is not None:
                self.sd.close()
        except Exception as e:
            # Swallow the exception if the socket is already closed
            logger.info(f"Error closing socket: {e}")
        finally:
            self.sd = None
