import asyncio

from pymemcache.client import base
from redis.client import Redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError


class StorageFabric:

    @classmethod
    def get_client(cls, name: str, addr: str) -> base.Client | Redis:

        if name == "memcache":
            return base.Client(addr)
        elif name == "redis":
            return Redis(
                host=addr.split(":")[0],
                port=int(addr.split(":")[1]),
                socket_timeout=3,
                retry=Retry(ExponentialBackoff(), 3),
                retry_on_error=[BusyLoadingError, ConnectionError, TimeoutError],
                decode_responses=True,
            )
        else:
            raise ValueError("Unexpected storage name")


class StorageManager:
    clients = dict()

    @classmethod
    def get_client(cls, addr: str) -> base.Client | Redis:
        if not cls.clients.get(addr):
            cls.clients[addr] = StorageFabric.get_client("redis", addr)
        return cls.clients.get(addr)

    @classmethod
    def set(cls, addr: str, key: str, value: str) -> None:
        cls.get_client(addr).set(key, value)

    @classmethod
    def get(cls, addr: str, key: str) -> str:
        return cls.get_client(addr).get(key)

