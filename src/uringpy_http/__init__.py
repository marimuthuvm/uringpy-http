"""uringpy-http: an ASGI server on io_uring with a C HTTP/1.1 parser.

The transport and the parser run in C with the GIL released; the GIL is
taken once per batch of requests (or once per request) to run the app.
"""
from ._core import ClientDisconnected, Engine
from .server import gil_enabled, make_listener, run

__version__ = "0.1.0"
__all__ = ["Engine", "ClientDisconnected", "run", "make_listener", "gil_enabled"]
