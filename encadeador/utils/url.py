import base62  # type: ignore


def base62_encode(content: str) -> str:
    return base62.encodebytes(content.encode("utf-8"))
