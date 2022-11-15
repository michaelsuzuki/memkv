from dataclasses import dataclass
from enum import Enum, IntEnum
from functools import reduce
from operator import concat
import struct
import sys
from typing import Optional, Sequence, Tuple, Union
import memkv.protocol.memkv_pb2 as memkv_pb2


# Classes
class InvalidMessageTypeError(Exception): pass


class MessageType(IntEnum):
    GET_COMMAND = 1
    SET_COMMAND = 2
    DELETE_COMMAND = 3
    METRICS_COMMAND = 4
    RESPONSE = 5


@dataclass
class MessageHeader(object):
    message_type: MessageType
    message_size: int


@dataclass
class MessageWrapper(object):
    header: MessageHeader
    data: bytes


# Constants
HEADER_SIZE = 6 # Header is 6 bytes: 2 byte unsigned short for message type and 4 byte unsigned long for size


# Types
MessageT = Union[
    memkv_pb2.GetCommand, memkv_pb2.SetCommand, memkv_pb2.DeleteCommand, memkv_pb2.MetricsCommand, memkv_pb2.Response
]
HeaderT = bytes
DataT = bytes


MESSAGE_CONSTRUCTORS = {
    MessageType.GET_COMMAND: lambda: memkv_pb2.GetCommand(),
    MessageType.SET_COMMAND: lambda: memkv_pb2.SetCommand(),
    MessageType.DELETE_COMMAND: lambda: memkv_pb2.DeleteCommand(),
    MessageType.METRICS_COMMAND: lambda: memkv_pb2.MetricsCommand(),
    MessageType.RESPONSE: lambda: memkv_pb2.Response()
}


def get_type(command: MessageT) -> int:
    if isinstance(command, memkv_pb2.GetCommand):
        return MessageType.GET_COMMAND
    elif isinstance(command, memkv_pb2.SetCommand):
        return MessageType.SET_COMMAND
    elif isinstance(command, memkv_pb2.DeleteCommand):
        return MessageType.DELETE_COMMAND
    elif isinstance(command, memkv_pb2.MetricsCommand):
        return MessageType.METRICS_COMMAND
    elif isinstance(command, memkv_pb2.Response):
        return MessageType.RESPONSE
    else:
        raise InvalidMessageTypeError("Invalid message found unable to get it's type")


def construct_header_and_data(command: MessageT) -> Tuple[HeaderT, DataT]:
    data = command.SerializeToString()
    size = len(data)
    header = struct.pack("!HI", get_type(command), size)
    return header, data


def decode_header(encoded_header: bytes) -> MessageHeader:
    message_type, size = struct.unpack("!HI", encoded_header)
    return MessageHeader(message_type=MessageType(message_type), message_size=size)

def construct_message(msg: MessageWrapper) -> Optional[MessageT]:
    global MESSAGE_CONSTRUCTORS
    message = None
    if msg.header.message_type in MESSAGE_CONSTRUCTORS:
        message = MESSAGE_CONSTRUCTORS[msg.header.message_type]()
        message.ParseFromString(msg.data)
    return message


def new_message_wrapper(header_bytes: bytes, data_bytes: bytes) -> MessageWrapper:
    return MessageWrapper(header=decode_header(header_bytes), data=data_bytes)


def encode_str(value: str) -> bytes:
    return value.encode("utf-8")


# Duplicated from: https://www.tutorialsteacher.com/articles/how-to-flatten-list-in-python
def flatten(items: list[any]) -> list[any]:
    return reduce(concat, items)

