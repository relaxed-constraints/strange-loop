"""Value codecs used by the SQLite run store."""

import base64
import json
import pickle
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol


class Codec(Protocol):
    name: str
    type: type

    def save(self, value: Any) -> bytes: ...
    def load(self, data: bytes) -> Any: ...


@dataclass(kw_only=True)
class CustomCodec[T]:
    type: type[T]
    name: str
    save: Callable[[T], bytes]
    load: Callable[[bytes], T]


class FileCodec:
    def __init__(self, cls: type, *, name: str, suffix: str = "") -> None:
        self.type = cls
        self.name = name
        self.suffix = suffix
        if "/" in suffix or "\\" in suffix:
            raise ValueError("suffix must be a filename suffix")

    def save(self, value: Any) -> bytes:
        with TemporaryDirectory() as directory:
            path = Path(directory) / ("value" + self.suffix)
            self.type.save(value, path)
            return path.read_bytes()

    def load(self, data: bytes) -> Any:
        with TemporaryDirectory() as directory:
            path = Path(directory) / ("value" + self.suffix)
            path.write_bytes(data)
            return self.type.load(path)


def _json_value(value: object) -> bool:
    if type(value) in (str, int, float, bool, type(None)):
        return True
    if type(value) is list:
        return all(map(_json_value, value))
    if type(value) is dict:
        return all(type(k) is str and _json_value(v) for k, v in value.items())
    return False


class Codecs:
    builtin = {"json", "tuple", "bytes", "pickle", "collection"}

    def __init__(self, codecs: Iterable[Codec], allow_pickle: bool) -> None:
        self.custom: dict[str, Codec] = {}
        self.allow_pickle = allow_pickle
        for codec in codecs:
            if not codec.name or codec.name in self.builtin or codec.name in self.custom:
                raise ValueError(f"Unavailable codec name: {codec.name!r}")
            self.custom[codec.name] = codec

    def encode(self, value: Any) -> tuple[str, bytes]:
        for codec in self.custom.values():
            if isinstance(value, codec.type):
                data = codec.save(value)
                if not isinstance(data, bytes):
                    raise TypeError(f"{codec.name}.save must return bytes")
                return codec.name, data
        if _json_value(value):
            return "json", json.dumps(value).encode()
        if type(value) is bytes:
            return "bytes", value
        if type(value) is tuple and all(map(_json_value, value)):
            return "tuple", json.dumps(value).encode()
        if type(value) in (tuple, list, dict):
            # Encode children independently when a container mixes formats.
            def pack(item: Any) -> list[str]:
                name, data = self.encode(item)
                return [name, base64.b64encode(data).decode("ascii")]

            entries = ([[pack(k), pack(v)] for k, v in value.items()] if type(value) is dict
                       else [pack(v) for v in value])
            return "collection", json.dumps([type(value).__name__, entries]).encode()
        try:
            return "pickle", pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as error:
            raise TypeError(f"No codec for {type(value).__qualname__} and pickle failed. "
                            "Register a CustomCodec or FileCodec.") from error

    def decode(self, name: str, data: bytes) -> Any:
        if name in self.custom:
            return self.custom[name].load(data)
        if name == "json":
            return json.loads(data)
        if name == "tuple":
            return tuple(json.loads(data))
        if name == "bytes":
            return data
        if name == "collection":
            def unpack(item: list[str]) -> Any:
                codec, encoded = item
                return self.decode(codec, base64.b64decode(encoded))
            kind, entries = json.loads(data)
            if kind == "dict":
                return {unpack(k): unpack(v) for k, v in entries}
            values = [unpack(v) for v in entries]
            return tuple(values) if kind == "tuple" else values
        if name == "pickle" and self.allow_pickle:
            return pickle.loads(data)
        raise ValueError(f"Codec {name!r} is unavailable. Register or enable it to read this value.")
