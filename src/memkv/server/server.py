import asyncio
import logging
import socket
from concurrent.futures import ThreadPoolExecutor
from functools import partial, reduce
from typing import Final, Optional

import memkv.protocol.memkv_pb2 as pb2
from memkv.protocol.util import (
    HEADER_SIZE,
    MessageWrapper,
    construct_message,
    decode_header,
    encode_into_header_and_data_bytes,
)
from memkv.server.locks import ReaderWriterLock, ReadLock, WriteLock

logger = logging.getLogger(__name__)

KEY_COUNT_METRIC: Final[str] = "key_count"
TOTAL_STORE_CONTENTS_SIZE_METRIC: Final[str] = "total_store_contents_size"
KEYS_READ_COUNT_METRIC: Final[str] = "keys_read_count"
KEYS_UPDATED_COUNT_METRIC: Final[str] = "keys_updated_count"
KEYS_DELETED_COUNT_METRIC: Final[str] = "keys_deleted_count"


class ServerMetrics(object):
    def __init__(self):
        self.metrics = {}
        self.rw_lock = ReaderWriterLock()

    def increment(self, metric_name: str, increment_by: int = 1) -> None:
        with WriteLock(self.rw_lock):
            if metric_name in self.metrics:
                self.metrics[metric_name] = self.metrics[metric_name] + increment_by
            else:
                self.metrics[metric_name] = increment_by

    def decrement(self, metric_name: str, decrement_by: int = 1) -> None:
        with WriteLock(self.rw_lock):
            if metric_name in self.metrics:
                self.metrics[metric_name] = self.metrics[metric_name] - decrement_by
            else:
                self.metrics[metric_name] = decrement_by

    def get(self, metric_name: str) -> Optional[int]:
        with ReadLock(self.rw_lock):
            return self.metrics[metric_name] if metric_name in self.metrics else None


class Server(object):
    def __init__(self, worker_count: int = 10, port: int = 9001):
        self.worker_count = worker_count
        self.port = port
        self.pool = ThreadPoolExecutor(worker_count)
        self.kv_rw_lock = ReaderWriterLock()
        self.key_value_store = {}
        self.metrics = ServerMetrics()
        self.terminated = False

    async def run(self):
        server = await asyncio.start_server(self.run_io_loop, host="127.0.0.1", port=self.port)
        async with server:
            await server.serve_forever()

    def execute_get(self, cmd: pb2.GetCommand) -> pb2.Response:
        logger.info(f"Executing GET command with {len(cmd.keys)} keys")
        with ReadLock(self.kv_rw_lock):
            key_values = {
                key: self.key_value_store[key]
                for key in cmd.keys
                if key in self.key_value_store
            }
        self.metrics.increment(KEYS_READ_COUNT_METRIC, len(cmd.keys))
        kv_list = pb2.KeyValueList()
        kv_list.key_values.extend(
            [pb2.KeyValue(key=k, value=v) for k, v in key_values.items()]
        )
        return pb2.Response(status="OK", message="OK", kv_list=kv_list)

    def execute_set(self, cmd: pb2.SetCommand) -> pb2.Response:
        logger.info(f"Executing SET command with {len(cmd.key_values)} key values")
        updates_dict = {kv.key: kv.value for kv in cmd.key_values}
        key_list = pb2.KeyList()
        with WriteLock(self.kv_rw_lock):
            last_total_size = reduce(
                lambda a, b: a + b, 
                [len(self.key_value_store.get(key, b"")) for key in updates_dict.keys()]
            )
            self.key_value_store.update(updates_dict)
        new_total_size = reduce(lambda a, b: a + b, [len(v) for v in updates_dict.values()])
        self.metrics.increment(KEYS_UPDATED_COUNT_METRIC, len(updates_dict))
        self.metrics.increment(TOTAL_STORE_CONTENTS_SIZE_METRIC, new_total_size - last_total_size)
        key_list.keys.extend([kv.key for kv in cmd.key_values])
        return pb2.Response(status="OK", message="OK", key_list=key_list)

    def execute_delete(self, cmd: pb2.DeleteCommand) -> pb2.Response:
        logger.info(f"Executing DELETE command with {len(cmd.keys)} keys")
        try:
            key_list = pb2.KeyList()
            keys_deleted = set()
            size_deleted = 0
            with WriteLock(self.kv_rw_lock):
                for key in cmd.keys:
                    value = self.key_value_store.pop(key, None)
                    if value is not None:
                        size_deleted += len(value)
                        keys_deleted.add(key)
            self.metrics.decrement(KEYS_DELETED_COUNT_METRIC, len(keys_deleted))
            self.metrics.decrement(TOTAL_STORE_CONTENTS_SIZE_METRIC, size_deleted)
            key_list.keys.extend([k for k in keys_deleted])
            kw_args = {"key_list": key_list} if len(key_list.keys) > 0 else {}
            return pb2.Response(status="OK", message="OK", **kw_args)
        except Exception as e:
            logger.exception(e)
            raise e

    def execute_metrics(self, cmd: pb2.DeleteCommand) -> pb2.Response:
        logger.info("Executing METRICS command")
        metrics = pb2.MetricsResponse()
        with ReadLock(self.kv_rw_lock):
            if cmd.get_key_count:
                metrics.key_count = len(self.key_value_store)

            if (
                cmd.get_total_store_contents_size
                and TOTAL_STORE_CONTENTS_SIZE_METRIC in self.metrics.metrics
            ):
                metrics.total_store_contents_size = self.metrics.get(
                    TOTAL_STORE_CONTENTS_SIZE_METRIC
                )

            if (
                cmd.get_keys_read_count
                and KEYS_READ_COUNT_METRIC in self.metrics.metrics
            ):
                metrics.keys_read_count = self.metrics.get(KEYS_READ_COUNT_METRIC)

            if (
                cmd.get_keys_updated_count
                and KEYS_UPDATED_COUNT_METRIC in self.metrics.metrics
            ):
                metrics.keys_updated_count = self.metrics.get(KEYS_UPDATED_COUNT_METRIC)

            if (
                cmd.get_keys_deleted_count
                and KEYS_DELETED_COUNT_METRIC in self.metrics.metrics
            ):
                metrics.keys_deleted_count = self.metrics.get(KEYS_DELETED_COUNT_METRIC)
        return pb2.Response(status="OK", message="OK", metrics=metrics)

    async def run_io_loop(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        logger.info("New client connected")
        while not self.terminated:
            try:
                response = await self.handle_message(reader)
                await self.handle_response(response, writer)
            except asyncio.IncompleteReadError:
                logger.info("Looks like the client was dropped or disconnected.")
                self._close_stream(writer)
            except socket.error as e:
                logger.exception(e)
                self._close_stream(writer)
                raise
            except Exception as e:
                logger.exception(e)
                self._close_stream(writer)
                raise

    async def handle_message(self, reader: asyncio.StreamReader) -> pb2.Response:
        header_bytes = await reader.readexactly(HEADER_SIZE)
        header = decode_header(header_bytes)
        data = await reader.readexactly(header.message_size)
        mw = MessageWrapper(header=header, data=data)
        running_loop = asyncio.get_event_loop()
        return await running_loop.run_in_executor(
            self.pool, partial(self.unwrap_message_and_execute, mw)
        )

    async def handle_response(
        self, response: pb2.Response, writer: asyncio.StreamWriter
    ) -> None:
        header, data = encode_into_header_and_data_bytes(response)
        writer.write(header)
        writer.write(data)
        await writer.drain()

    def terminate(self):
        self.terminated = True

    def unwrap_message_and_execute(self, msg_wrapper: MessageWrapper) -> pb2.Response:
        try:
            msg = construct_message(msg_wrapper)
            if isinstance(msg, pb2.GetCommand):
                return self.execute_get(msg)
            elif isinstance(msg, pb2.SetCommand):
                return self.execute_set(msg)
            elif isinstance(msg, pb2.DeleteCommand):
                return self.execute_delete(msg)
            elif isinstance(msg, pb2.MetricsCommand):
                return self.execute_metrics(msg)
            else:
                raise Exception(
                    f"Unexpected message type received {msg.__class__.__name__}"
                )
        except Exception as e:
            logger.exception("Error executing a message")
            return pb2.Response(status="ERROR", message=str(e))

    async def _close_stream(writer: asyncio.StreamWriter) -> None:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            logger.warn(f"Error trying to close the stream: {e}")
