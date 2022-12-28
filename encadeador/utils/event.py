from typing import Any


class Event(list):
    async def __call__(self, *args: Any, **kwds: Any) -> Any:
        for item in self:
            await item(*args, **kwds)
