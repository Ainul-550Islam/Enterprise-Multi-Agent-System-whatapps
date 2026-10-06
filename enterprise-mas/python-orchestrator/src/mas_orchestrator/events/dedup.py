"""Event deduplication.

Python may receive duplicate deliveries (spec §16), and an event consumer that
applies the same event twice can produce a duplicate durable outcome. This
module answers one question — "have we seen this event?" — and it is explicit
that the answer is best-effort.

It is **not** a competing idempotency system. Rust owns authoritative execution
state; this only suppresses repeated *emissions* so a downstream consumer sees
each event once. Anything that must happen exactly once has to be driven by
Rust's state, not by this cache.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import InvalidRequestError

if TYPE_CHECKING:
    from mas_orchestrator.events.models import EventEnvelope
    from mas_orchestrator.events.types import ExecutionEvent

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

DEFAULT_WINDOW_SECONDS: Final[float] = 300.0
DEFAULT_MAX_KEYS: Final[int] = 10_000


class EventDeduplicator:
    """Remembers recently seen event keys within a time window.

    The window is what keeps this from being an unbounded memory leak. An entry
    older than the window is evicted on access, and eviction is what makes the
    answer best-effort: a duplicate that arrives after the window has passed
    will be emitted again.
    """

    def __init__(
        self,
        *,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        max_keys: int = DEFAULT_MAX_KEYS,
    ) -> None:
        if window_seconds <= 0:
            raise InvalidRequestError(
                "window must be positive", details={"field": "window_seconds"}
            )
        if max_keys < 1:
            raise InvalidRequestError("max_keys must be positive", details={"field": "max_keys"})
        self._window = window_seconds
        self._max_keys = max_keys
        self._seen: dict[str, float] = {}

    def is_duplicate(self, event: ExecutionEvent) -> bool:
        """Whether this event was already seen inside the window.

        Calling this records the event, so the first call returns ``False`` and
        a second call for the same event returns ``True``.
        """
        return self._is_duplicate(self.key_for(event))

    def is_duplicate_envelope(self, envelope: EventEnvelope) -> bool:
        """Whether an envelope was already seen inside the window.

        A separate entry point rather than a coercion inside
        :meth:`is_duplicate`, because a consumer sees envelopes and an emitter
        sees events, and the two must not be confused: an envelope rebuilt from
        a broker message is not the event that produced it.
        """
        return self._is_duplicate(self.envelope_key(envelope))

    def envelope_key(self, envelope: EventEnvelope) -> str:
        """Key for an envelope, built from the same parts as :meth:`key_for`."""
        return "|".join(
            (
                envelope.event_name,
                envelope.tenant_id,
                envelope.execution_id,
                envelope.correlation_id,
                str(envelope.occurred_epoch_ms),
            )
        )

    def _is_duplicate(self, key: str) -> bool:
        now = time.monotonic()
        self._evict(now)

        previous = self._seen.get(key)
        if previous is not None and now - previous <= self._window:
            _LOGGER.debug(
                "duplicate event suppressed",
                extra={"event": "events.duplicate", "key": key},
            )
            return True

        if len(self._seen) >= self._max_keys:
            # Bound the map even if nothing is stale yet: dropping the oldest
            # entry is preferable to growing without limit.
            oldest = min(self._seen, key=self._seen.__getitem__)
            del self._seen[oldest]

        self._seen[key] = now
        return False

    def key_for(self, event: ExecutionEvent) -> str:
        """Stable key identifying an event.

        The identity is the event name plus the correlation context plus the
        occurred timestamp. Two genuinely distinct events at the same
        millisecond with the same name would collapse — which is accepted,
        because the alternative is to claim an exactness this cache cannot
        provide.
        """
        return (
            f"{event.name.value}"
            f"|{event.tenant_id}"
            f"|{event.execution_id}"
            f"|{event.correlation_id}"
            f"|{event.occurred_epoch_ms}"
        )

    def _evict(self, now: float) -> None:
        """Drops entries older than the window."""
        stale = [key for key, seen_at in self._seen.items() if now - seen_at > self._window]
        for key in stale:
            del self._seen[key]

    def __len__(self) -> int:
        """How many keys are currently held."""
        return len(self._seen)
