"""A Starlette route that returns JSON built from the path and query."""
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route


async def item(request):
    return JSONResponse({"id": int(request.path_params["item_id"]),
                         "q": request.query_params.get("q"),
                         "ok": True})


async def root(request):
    return JSONResponse({"message": "Hello, world!"})


app = Starlette(routes=[Route("/", root), Route("/items/{item_id:int}", item)])
