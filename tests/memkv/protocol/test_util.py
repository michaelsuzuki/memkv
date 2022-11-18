import struct
import sys
from typing import Callable

import pytest

import memkv.protocol.memkv_pb2 as memkv_pb2
from memkv.protocol.util import (
    HEADER_SIZE,
    InvalidMessageTypeError,
    MessageHeader,
    MessageT,
    MessageWrapper,
    construct_message,
    decode_header,
    encode_into_header_and_data_bytes,
    encode_str,
    get_type,
)


def get_command() -> memkv_pb2.GetCommand:
    return memkv_pb2.GetCommand(keys=["testKeyOne"])


def set_command() -> memkv_pb2.SetCommand:
    kv_list = [
        memkv_pb2.KeyValue(key="testKeyOne", value=encode_str("This is a test value")),
        memkv_pb2.KeyValue(key="testKeyTwo", value=encode_str("Another test value"))
    ]
    command = memkv_pb2.SetCommand()
    command.key_values.extend(kv_list)
    return command


def delete_command() -> memkv_pb2.DeleteCommand:
    return memkv_pb2.DeleteCommand(keys=["testKeyOne"])


def response() -> memkv_pb2.Response:
    return memkv_pb2.Response(status="success", message="Success")


def metrics_command() -> memkv_pb2.MetricsCommand:
    return memkv_pb2.MetricsCommand(get_key_count=True, get_total_store_contents_size=True)


def wrap_message(msg: MessageT) -> MessageWrapper:
    header_bytes, data = encode_into_header_and_data_bytes(msg)
    header = decode_header(header_bytes)
    return MessageWrapper(header=header, data=data)


def assert_result(header: bytes, data: bytes, proto_obj: MessageT, expected: MessageT):
    assert HEADER_SIZE == len(header), f"Header length should be 6 bytes found {len(header)}"
    try:
        proto_obj.ParseFromString(data)
        assert proto_obj.SerializeToString(deterministic=True) == \
            expected.SerializeToString(deterministic=True)
        print(f"Parsing Data succeeded for {proto_obj}", file=sys.stderr)
    except Exception:
        pytest.fail()


@pytest.mark.parametrize("command, assertion", [
    (get_command(), lambda h, d, c: assert_result(h, d, memkv_pb2.GetCommand(), c)),
    (set_command(), lambda h, d, c: assert_result(h, d, memkv_pb2.SetCommand(), c)),
    (delete_command(), lambda h, d, c: assert_result(h, d, memkv_pb2.DeleteCommand(), c)),
    (metrics_command(), lambda h, d, c: assert_result(h, d, memkv_pb2.MetricsCommand(), c)),
    (response(), lambda h, d, c: assert_result(h, d, memkv_pb2.Response(), c)),
])
def test_construct_header_and_data(command: MessageT, assertion: Callable[[bytes, bytes, MessageT], None]):
    header, data = encode_into_header_and_data_bytes(command)
    assertion(header, data, command)


def test_construct_header_and_data_with_bad_message_type():
    class BadType(object):
        pass

    with pytest.raises((AttributeError, InvalidMessageTypeError)):
        bad_obj = BadType()
        encode_into_header_and_data_bytes(bad_obj)


@pytest.mark.parametrize("command", [
    get_command(),
    set_command(),
    delete_command(),
    metrics_command(),
    response(),
])
def test_decode_header(command: MessageT):
    header_bytes, data_bytes = encode_into_header_and_data_bytes(command)
    header = decode_header(header_bytes)
    expected = len(data_bytes)
    found = header.message_size
    assert found == expected, f"Message size was expected to be {expected} but was found to be {found}"


def test_decode_header_with_bad_byte_order():
    command = get_command()
    header = struct.pack("=HI", get_type(command), 1000)
    assert len(header) == 6, f"Found {len(header)}, expected: 6"
    with pytest.raises(ValueError):
        decode_header(header)


def test_decode_header_with_too_few_bytes():
    command = get_command()
    header = struct.pack("=HH", get_type(command), 1000)
    assert len(header) == 4
    with pytest.raises(struct.error):
        decode_header(header)


@pytest.mark.parametrize("message", [
    wrap_message(get_command()),
    wrap_message(set_command()),
    wrap_message(delete_command()),
    wrap_message(metrics_command()),
    wrap_message(response()),
])
def test_construct_command(message: MessageWrapper):
    command = construct_message(message)
    assert command is not None


def test_construct_command_with_unknown_type():
    with pytest.raises((AttributeError, InvalidMessageTypeError)):
        header = MessageHeader(message_type=20, message_size=100)
        msg = MessageWrapper(header=header, data=b"abcde")
        encode_into_header_and_data_bytes(msg)
