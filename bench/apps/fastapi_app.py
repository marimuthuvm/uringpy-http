"""A FastAPI route with a typed path parameter, a query parameter and a
pydantic response model: what a small JSON API endpoint looks like."""
from fastapi import FastAPI
from pydantic import BaseModel


class Item(BaseModel):
    id: int
    q: str | None = None
    ok: bool = True


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello, world!"}


@app.get("/items/{item_id}", response_model=Item)
async def item(item_id: int, q: str | None = None):
    return Item(id=item_id, q=q)
