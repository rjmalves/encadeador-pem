import base62


def base62_encode(content: str) -> str:
    return base62.encodebytes(content.encode("utf-8"))
