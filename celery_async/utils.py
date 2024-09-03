import functools
import asyncio


def to_async(func):
    if asyncio.iscoroutinefunction(func):
        return func

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper