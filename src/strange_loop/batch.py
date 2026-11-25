"""Per-attempt batch inputs and the submission contract."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from .versions import RevisionRequest, Version

type BatchResult = dict[str, Any] | None
type BatchFunction = Callable[[Batch], BatchResult | Awaitable[BatchResult]]
type Accept = Callable[[object], Version]
type Deliver = Callable[[str, object], None]
type Pending = tuple[
    dict[str, object], RevisionRequest | None, Accept, asyncio.Future[Version]
]


@dataclass(frozen=True)
class BatchItem:
    """One pending call in a batch attempt.

    Attributes:
        key: Key used to match a submission with its caller.
        inputs: Named stage inputs with versions resolved to their values.
        request: Revision details, or None for an initial call.
    """

    key: str
    inputs: dict[str, Any]
    request: RevisionRequest | None
    _batch: Batch

    async def submit(self, value: object) -> None:
        """Accept this item’s output before the batch function returns.

        Args:
            value: Output for the caller associated with this item.

        Raises:
            BatchContractError: The attempt ended or this key already received a different value.
        """
        self._batch._accept(self.key, value)


class BatchContractError(ValueError):
    """A batch attempt violated the submission contract, such as submitting and returning a dictionary."""


class Batch:
    """Items scheduled together for one attempt. Iterate over items, index by key, or use len(batch)."""

    def __init__(self, pending: Mapping[str, Pending], accept: Deliver) -> None:
        self._items = {
            key: BatchItem(key, inputs, request, self)
            for key, (inputs, request, _, _) in pending.items()
        }
        self._deliver: Deliver | None = accept
        self._accepted: dict[str, object] = {}

    def __iter__(self) -> Iterator[BatchItem]:
        return iter(self._items.values())

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, key: str) -> BatchItem:
        return self._items[key]

    def _accept(self, key: str, value: object) -> None:
        if self._deliver is None:
            raise BatchContractError("This batch attempt has ended.")
        if key in self._accepted:
            if self._accepted[key] != value:
                raise BatchContractError(f"Conflicting submissions for {key!r}.")
            return
        self._deliver(key, value)
        self._accepted[key] = value

    def _finish(self, outputs: object) -> None:
        if outputs is None:
            return
        if not isinstance(outputs, dict):
            raise BatchContractError(
                "Return a keyed dictionary or submit items and return None."
            )
        if self._accepted:
            raise BatchContractError(
                "This batch stage submitted an item and also returned a dictionary. "
                "Submit the remaining items and return None, or return all results in one dictionary."
            )
        for key in outputs:
            if key not in self._items:
                raise BatchContractError(f"Unexpected submission for {key!r}.")
        for key, value in outputs.items():
            self._accept(key, value)

    def _close(self) -> None:
        self._deliver = None
