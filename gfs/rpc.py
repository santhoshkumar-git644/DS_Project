"""Thin gRPC layer: unary calls with a JSON codec, so no protoc code generation is needed.

Every node exposes one service, /gfs.Node/<Method>, whose request and response are JSON objects.
"""
import json
import threading
from concurrent import futures

import grpc

from . import config

OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 400),
    ("grpc.max_send_message_length", 32 << 20),
    ("grpc.max_receive_message_length", 32 << 20),
]


class RpcError(Exception):
    """The peer was unreachable, timed out, or failed inside the handler."""


def _dumps(obj):
    return json.dumps(obj, separators=(",", ":")).encode()


def _loads(raw):
    return json.loads(raw.decode())


_lock = threading.Lock()
_channels = {}
_callables = {}

# Simulated network partition: calls from this process to these addresses fail as if the network
# dropped them. Set on both sides of a cut, so no RPC crosses it in either direction.
BLOCKED = set()


def _callable(addr, method):
    key = (addr, method)
    with _lock:
        fn = _callables.get(key)
        if fn is None:
            ch = _channels.get(addr)
            if ch is None:
                ch = grpc.insecure_channel(addr, options=OPTIONS)
                _channels[addr] = ch
            fn = ch.unary_unary(f"/gfs.Node/{method}", request_serializer=_dumps,
                                response_deserializer=_loads)
            _callables[key] = fn
        return fn


def call(addr, method, body=None, timeout=1.0):
    if addr in BLOCKED:
        raise RpcError(f"{addr} {method}: PARTITIONED")
    try:
        resp = _callable(addr, method)(body or {}, timeout=timeout)
    except grpc.RpcError as e:
        raise RpcError(f"{addr} {method}: {e.code().name}") from None
    if isinstance(resp, dict) and "rpc_error" in resp:
        raise RpcError(f"{addr} {method}: {resp['rpc_error']}")
    return resp


class _Handler(grpc.GenericRpcHandler):
    def __init__(self, methods):
        self._methods = methods

    def service(self, details):
        fn = self._methods.get(details.method.rsplit("/", 1)[-1])
        if fn is None:
            return None

        def behavior(request, context):
            try:
                return fn(request)
            except Exception as e:  # keep the connection alive; report to caller
                return {"rpc_error": f"{type(e).__name__}: {e}"}

        return grpc.unary_unary_rpc_method_handler(
            behavior, request_deserializer=_loads, response_serializer=_dumps)


def serve(port, methods, workers=48):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers), options=OPTIONS)
    server.add_generic_rpc_handlers((_Handler(methods),))
    if server.add_insecure_port(config.addr(port)) == 0:
        raise RuntimeError(f"cannot bind port {port}")
    server.start()
    return server
