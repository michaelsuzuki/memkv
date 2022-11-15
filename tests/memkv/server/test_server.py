from asyncio import AbstractEventLoop, StreamReader
import pytest

import memkv.protocol.memkv_pb2 as pb2
from memkv.protocol.util import MessageT, construct_header_and_data
from memkv.server.server import Server


def get_command() -> pb2.GetCommand:
    return pb2.GetCommand(keys=["keyOne", "keyTwo"])


def set_command() -> pb2.SetCommand:
    kv_list = [
        pb2.KeyValue(key="keyTwo", value=b"valueTwo"),
        pb2.KeyValue(key="keyThree", value=b"valueThree"),
        pb2.KeyValue(key="keyFour", value=b"valueFour")
    ]
    return pb2.SetCommand(key_values=kv_list)


def metrics_command() -> pb2.MetricsCommand:
    return pb2.MetricsCommand(
        get_key_count = True,
        get_total_store_size = True,
        get_get_command_count = True,
        get_set_command_count = True,
        get_delete_command_count = True,
    )


def delete_command() -> pb2.DeleteCommand:
    return pb2.DeleteCommand(keys=["keyFour"])


def message_reader(loop: AbstractEventLoop, msg: MessageT) -> StreamReader:
    header, data = construct_header_and_data(msg)    
    reader = StreamReader(loop=loop)
    reader.feed_data(header)
    reader.feed_data(data)
    return reader


def assert_correct_response(server: Server, msg: MessageT, response: pb2.Response):
    if isinstance(msg, pb2.GetCommand):
        kv_dict = {kv.key: kv.value for kv in response.kv_list.key_values}
        for key in msg.keys:
            assert key in kv_dict, f"No value returned for '{key}' should've gotten '{kv_dict[key]}'"
            expected_value = server.key_value_store[key]
            actual_value = kv_dict[key]
            assert expected_value == actual_value, f"Response value: {actual_value} is not the actual value: {expected_value}"
    elif isinstance(msg, pb2.SetCommand):
        resp_keys_set = {k for k in response.key_list.keys}
        msg_keys_dict = {kv.key: kv.value for kv in msg.key_values}
        for key, actual_value in msg_keys_dict.items():
            assert key in resp_keys_set, f"Key {key} in msg is not in response"
            expected_value = server.key_value_store[key] if key in server.key_value_store else None
            assert actual_value == expected_value, f"The expected value: {expected_value} does not match the actual value: {actual_value}"
    elif isinstance(msg, pb2.DeleteCommand):
        response_keys = {key for key in response.key_list.keys}
        msg_keys = {k for k in msg.keys}
        assert response_keys == msg_keys, f"The keys to be deleted = {msg_keys} don't match the ones returned: {response_keys}"
    elif isinstance(msg, pb2.MetricsCommand):


@pytest.mark.asyncio
@pytest.mark.parametrize("msg" , [
    get_command(),
    set_command(),
    delete_command(),
])
async def test_handle_message_types(msg: MessageT, event_loop: AbstractEventLoop):
    server = Server()
    try:
        reader = message_reader(event_loop, msg)
        server.key_value_store["keyOne"] = b"valueOne"
        server.key_value_store["keyTwo"] = b"valueTwo"
        server.key_value_store["KeyFour"] = b"valueFour"
        response = await server.handle_message(reader)
        assert_correct_response(server, msg, response)
    finally:
        server.terminate()
    
