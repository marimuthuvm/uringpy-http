"""The smallest ASGI app: one route, a fixed 13-byte body."""
BODY = b"Hello, world!"
START = {"type": "http.response.start", "status": 200,
         "headers": [(b"content-type", b"text/plain"), (b"content-length", b"13")]}
END = {"type": "http.response.body", "body": BODY}


async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await send(START)
    await send(END)
