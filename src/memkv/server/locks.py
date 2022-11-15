# This is an implementation of a cross process R/W Lock

from dataclasses import dataclass
from os import getpid
import threading
from typing import Dict, Set


@dataclass
class ThreadId(object):
    identifier: str
    def __init__(self):
        self.identifier = str(getpid()) + "." + str(threading.get_ident())

    def __hash__(self):
        return self.identifier.__hash__()


# This reader writer lock prioritizes writes over reads.
# Once a write comes in all new reads must wait for the
# write to complete.
class ReaderWriterLock(object):
    def __init__(self):
        self.rwlock = 0
        self.writes_waiting = 0
        self.lock = threading.Lock()
        self.readers_ok = threading.Condition(self.lock)
        self.writers_ok = threading.Condition(self.lock)

    def _can_read(self) -> bool:
        return False if self.writers else True

    def _can_write(self) -> bool:
        if self.rwlock != 0:
            self.writes_waiting += 1
        return self.rwlock != 0
    
    def read_acquire(self):
        with self.lock:
            while self.rwlock < 0 or self.writes_waiting:
                self.readers_ok.wait()
            self.rwlock += 1
    
    def write_acquire(self):
        with self.lock:
            while self.rwlock != 0:
                self.writes_waiting += 1
                self.writers_ok.wait()
                self.writes_waiting -= 1
            self.rwlock = -1

    def release(self):
        with self.lock:
            if self.rwlock < 0:
                self.rwlock = 0
            else:
                self.rwlock -= 1
            wake_writers = self.writes_waiting and self.rwlock == 0
            wake_readers = self.writes_waiting == 0
        if wake_writers:
            with self.writers_ok:
                self.writers_ok.notify()
        elif wake_readers:
            with self.readers_ok:
                self.readers_ok.notify_all()


class ReadLock(object):
    def __init__(self, lock: ReaderWriterLock):
        self.lock = lock

    def __enter__(self):
        self.lock.read_acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.lock.release()
        return False


class WriteLock(object):
    def __init__(self, lock: ReaderWriterLock):
        self.lock = lock

    def __enter__(self):
        self.lock.write_acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.lock.release()
        return False


