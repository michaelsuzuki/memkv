from functools import reduce
import pytest
from pytest_mock import MockerFixture

from memkv.client.api import Client, ClientAPIException
import memkv.protocol.memkv_pb2 as pb2
from memkv.protocol.util import encode_into_header_and_data_bytes


@pytest.fixture
def client(mocker: MockerFixture) -> Client:
    client = Client(host="127.0.0.1", port=9001)

    def patch_client():
        if client.sd is None:
            mocker.patch.object(client, "sd")
            mocker.patch.object(client, "connect")

    patch_client()
    client.connect.side_effect = patch_client
    return client


def construct_response(status: str, message: str, *keys, **kwargs) -> pb2.Response:
    if keys:
        key_list = pb2.KeyList(keys=keys)
        return pb2.Response(status=status, message=message, key_list=key_list)
    if kwargs:
        key_values = [pb2.KeyValue(key=k, value=v) for k, v in kwargs.items()]
        kv_list = pb2.KeyValueList(key_values=key_values)
        return pb2.Response(status=status, message=message, kv_list=kv_list)
    return pb2.Response(status=status, message=message)


def test_get_with_one_key(client):
    key_values = [pb2.KeyValue(key="keyOne", value=b"valueOne")]
    expected_response = pb2.Response(
        status="OK", message="OK", kv_list=pb2.KeyValueList(key_values=key_values)
    )
    header_bytes, data_bytes = encode_into_header_and_data_bytes(expected_response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    actual_key_values = client.get(["keyOne"])
    matches = [
        kv.key in actual_key_values and kv.value == actual_key_values[kv.key] for kv in key_values
    ]
    assert reduce(lambda a, b: a and b, matches), \
        "One of the keys or values does not match what was expected!"


def test_get_with_multiple_keys(client):
    expected_response = construct_response("OK", "OK", keyOne=b"valueOne", keyTwo=b"valueTwo")
    header_bytes, data_bytes = encode_into_header_and_data_bytes(expected_response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    actual_key_values = client.get(["keyOne", "keyTwo"])
    matches = [
        kv.key in actual_key_values and kv.value == actual_key_values[kv.key]
        for kv in expected_response.kv_list.key_values
    ]
    assert reduce(lambda a, b: a and b, matches), \
        "One of the keys or values does not match what was expected!"


def test_get_with_error_status(client):
    response = construct_response("ERROR", "An error happened!")
    header_bytes, data_bytes = encode_into_header_and_data_bytes(response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    with pytest.raises(ClientAPIException):
        client.get(["keyOne", "keyTwo", "keyThree"])


def test_set_with_one_key_value(client: Client):
    response = construct_response("OK", "OK", "keyOne")
    header_bytes, data_bytes = encode_into_header_and_data_bytes(response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    response_keys = [k for k in response.key_list.keys]
    actual = client.set({"keyOne": b"valueOne"})
    assert response_keys == actual


def test_set_with_multiple_key_values(client):
    response = construct_response("OK", "OK", "keyOne", "keyTwo")
    header_bytes, data_bytes = encode_into_header_and_data_bytes(response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    response_keys = [k for k in response.key_list.keys]
    actual = client.set({"keyOne": b"valueOne", "keyTwo": b"valueTwo"})
    assert response_keys == actual


def test_set_with_error_status(client):
    response = construct_response("ERROR", "An error happened!")
    header_bytes, data_bytes = encode_into_header_and_data_bytes(response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    with pytest.raises(ClientAPIException):
        client.set({"keyOne": b"valueOne", "keyTwo": b"valueTwo", "keyThree": b"valueThree"})


def test_delete_one_key(client):
    response = construct_response("OK", "OK", "keyOne")
    header_bytes, data_bytes = encode_into_header_and_data_bytes(response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    response_keys = [k for k in response.key_list.keys]
    actual = client.delete(["keyOne"])
    assert response_keys == actual


def test_delete_multiple_keys(client):
    response = construct_response("OK", "OK", "keyOne", "keyTwo")
    header_bytes, data_bytes = encode_into_header_and_data_bytes(response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    response_keys = [k for k in response.key_list.keys]
    actual = client.delete(["keyOne", "keyTwo"])
    assert response_keys == actual


def test_delete_with_error(client):
    response = construct_response("ERROR", "An error happened!")
    header_bytes, data_bytes = encode_into_header_and_data_bytes(response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    with pytest.raises(ClientAPIException):
        client.delete(["keyOne", "keyTwo", "keyThree"])


def test_metrics_command(client):
    metrics = pb2.MetricsResponse(
        key_count=10,
        total_store_contents_size=108,
        keys_read_count=15,
        keys_updated_count=12,
        keys_deleted_count=3
    )
    response = pb2.Response(status="OK", message="OK", metrics=metrics)
    header_bytes, data_bytes = encode_into_header_and_data_bytes(response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    actual_metrics = client.metrics()
    assert metrics.key_count == actual_metrics.key_count
    assert metrics.total_store_contents_size == actual_metrics.total_store_contents_size


def test_metrics_with_errors(client):
    response = construct_response("ERROR", "An error happened!")
    header_bytes, data_bytes = encode_into_header_and_data_bytes(response)
    client.sd.recv.side_effect = [header_bytes, data_bytes]
    with pytest.raises(ClientAPIException):
        client.metrics()
