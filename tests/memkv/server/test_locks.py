import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Any, Dict

from memkv.protocol.util import flatten
from memkv.server.locks import ReaderWriterLock, ReadLock, ThreadId, WriteLock


def read_from_dict(
    shared_dict: Dict[str, Any],
    key: str,
    lock: ReaderWriterLock,
    ops: Queue,
    sleep_interval: int = 0.0,
) -> Any:
    curr_id = ThreadId()
    with ReadLock(lock):
        print("{curr_id}: Acquired ReadLock", file=sys.stderr)
        if sleep_interval > 0:
            time.sleep(sleep_interval)
        print(f"{curr_id}: Waiting for ReadLock", file=sys.stderr)
        ops.put(("R", curr_id, shared_dict[key]))
        return "R", curr_id, shared_dict[key]


def add_dict_item(key: str, value: str, _dict: Dict[str, Any]):
    _dict[key] = value


def write_to_dict(
    shared_dict: Dict[str, Any],
    key: str,
    value: Any,
    lock: ReaderWriterLock,
    ops: Queue,
):
    curr_id = ThreadId()
    print(f"{curr_id} Waiting for WriteLock", file=sys.stderr)
    with WriteLock(lock):
        print(f"{curr_id} Acquired WriteLock", file=sys.stderr)
        shared_dict[key] = value
        ops.put(("W", curr_id, value))
        return "W", curr_id, value


def assert_read_and_writes_in_queue(queue: Queue, expected_write_count: int) -> None:
    results = [tup for tup in queue.queue]
    curr_write_value = ""
    write_occurred = False
    writes_counted = 0
    reads_before_any_writes = False
    reads_following_write = False
    for op, _, value in results:
        if op == "W":
            write_occurred = True
            writes_counted += 1
            reads_following_write = 0
            curr_write_value = value
        elif op == "R":
            if write_occurred:
                write_occurred = False
                reads_following_write += 1
                assert (
                    value == curr_write_value
                ), f"Expected value {curr_write_value} detected value: {value}"
            else:
                if reads_following_write > 0:
                    reads_following_write += 1
                    assert (
                        value == curr_write_value
                    ), f"Expected value {curr_write_value} detected value: {value}"
                else:
                    reads_before_any_writes += 1
    assert (
        writes_counted == expected_write_count
    ), f"Expected write count {expected_write_count}, detected {writes_counted}"


def test_reads_dont_block():
    # Using the same key, read the same value over and over from multiple threads
    rwl = ReaderWriterLock()
    pool = ThreadPoolExecutor(max_workers=10)
    shared_dict = {"foo": "bar"}
    ops = Queue()
    futures = [
        pool.submit(read_from_dict, d, k, l, ops)
        for d, k, l in ([[shared_dict, "foo", rwl]] * 50)
    ]
    for future in as_completed(futures):
        assert future.result()[2] == "bar"


def test_writes_block_reads():
    rwl = ReaderWriterLock()
    pool = ThreadPoolExecutor(max_workers=10)
    shared_dict = {"foo": "before"}
    ops = Queue()
    futures_1 = [
        pool.submit(read_from_dict, d, k, l, ops)
        for d, k, l in ([[shared_dict, "foo", rwl]] * 10)
    ]
    futures_2 = [
        pool.submit(read_from_dict, d, k, l, ops, 0.01)
        for d, k, l in ([[shared_dict, "foo", rwl]] * 5)
    ]
    futures_3 = [pool.submit(write_to_dict, shared_dict, "foo", "after", rwl, ops)]
    futures_4 = [
        pool.submit(read_from_dict, d, k, l, ops, 0.025)
        for d, k, l in ([[shared_dict, "foo", rwl]] * 10)
    ]
    futures = flatten([futures_1, futures_2, futures_3, futures_4])
    _ = [future.result() for future in as_completed(futures)]
    assert_read_and_writes_in_queue(ops, 1)


def test_multiple_writes_block_reads():
    rwl = ReaderWriterLock()
    pool = ThreadPoolExecutor(max_workers=10)
    shared_dict = {"foo": "before"}
    ops = Queue()
    futures_1 = [
        pool.submit(read_from_dict, d, k, l, ops)
        for d, k, l in ([[shared_dict, "foo", rwl]] * 10)
    ]
    futures_2 = [pool.submit(write_to_dict, shared_dict, "foo", "after", rwl, ops)]
    futures_3 = [
        pool.submit(read_from_dict, d, k, l, ops, 0.025)
        for d, k, l in ([[shared_dict, "foo", rwl]] * 10)
    ]
    futures_4 = [
        pool.submit(write_to_dict, shared_dict, "foo", "after_first", rwl, ops)
    ]
    futures_5 = [
        pool.submit(read_from_dict, d, k, l, ops)
        for d, k, l in ([[shared_dict, "foo", rwl]] * 10)
    ]
    futures = flatten([futures_1, futures_2, futures_3, futures_4, futures_5])
    _ = [future.result() for future in as_completed(futures)]
    assert_read_and_writes_in_queue(ops, 2)
