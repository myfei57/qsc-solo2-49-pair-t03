"""In-process publish/subscribe bus used for cross-area reactions."""

from __future__ import annotations

import logging
import threading
from collections import deque
from collections.abc import Callable

from .domain.events import Event, utcnow
from .domain.kinds import EventKind

Handler = Callable[[Event], None]

LOGGER = logging.getLogger("linecontrol.bus")

#: Number of recent events retained for late-joining subscribers.
HISTORY_LIMIT = 500

#: Number of recent delivery failures retained for inspection.
FAILURE_LIMIT = 100


class EventBus:
    """Delivers events to subscribers registered for the matching kind.

    Handlers are copied out of the registry before dispatch so that a handler
    may unsubscribe or publish a follow-up event without disturbing the
    current dispatch pass.  Each handler is invoked in isolation: a handler
    that raises is recorded in the failure log and the remaining handlers
    still receive the event, so one faulty subscriber cannot silence the
    audit trail or the console stream.

    The bus also keeps a bounded history of recently published events.  A
    subscriber that joins midway may pass ``replay`` to :meth:`subscribe` to
    catch up on the most recent segment, and :meth:`recent` exposes the same
    buffer for after-the-fact reconciliation.
    """

    def __init__(self, history_limit: int = HISTORY_LIMIT) -> None:
        self._lock = threading.Lock()
        self._next_id = 0
        self._subs: dict[EventKind, list[tuple[int, Handler]]] = {}
        self._delivered = 0
        self._history: deque[Event] = deque(maxlen=history_limit)
        self._failures: deque[dict[str, object]] = deque(maxlen=FAILURE_LIMIT)
        self._failed = 0

    def publish(self, event: Event) -> None:
        with self._lock:
            handlers = [handler for _, handler in self._subs.get(event.kind, ())]
            self._delivered += 1
            self._history.append(event)
        for handler in handlers:
            self._deliver(event, handler)

    def subscribe(
        self,
        kind: EventKind,
        handler: Handler,
        replay: int = 0,
    ) -> Callable[[], None]:
        """Register ``handler`` and optionally replay recent matching events.

        The subscription is registered before the backlog is replayed, and
        both happen under the same lock as publication, so an event is either
        part of the replayed backlog or delivered live afterwards — never
        both and never out of order.  Replay delivery is isolated the same
        way as live delivery.
        """

        with self._lock:
            self._next_id += 1
            token = self._next_id
            self._subs.setdefault(kind, []).append((token, handler))
            backlog = [event for event in self._history if event.kind is kind]
            if replay > 0:
                backlog = backlog[-replay:]
            else:
                backlog = []
        for event in backlog:
            self._deliver(event, handler)

        def unsubscribe() -> None:
            with self._lock:
                entries = self._subs.get(kind, [])
                self._subs[kind] = [item for item in entries if item[0] != token]

        return unsubscribe

    def subscribers(self, kind: EventKind) -> int:
        with self._lock:
            return len(self._subs.get(kind, ()))

    def delivered(self) -> int:
        with self._lock:
            return self._delivered

    def failed(self) -> int:
        """Total number of handler invocations that raised."""

        with self._lock:
            return self._failed

    def failures(self, limit: int = 20) -> list[dict[str, object]]:
        """Most recent delivery failures, oldest first."""

        with self._lock:
            entries = list(self._failures)
        return entries[-limit:]

    def recent(self, kind: EventKind | None = None, limit: int = 0) -> list[Event]:
        """Events from the retained history, optionally filtered by kind."""

        with self._lock:
            events = list(self._history)
        if kind is not None:
            events = [event for event in events if event.kind is kind]
        if limit > 0:
            events = events[-limit:]
        return events

    def _deliver(self, event: Event, handler: Handler) -> None:
        try:
            handler(event)
        except Exception as error:  # noqa: BLE001 - keep subscribers isolated
            self._record_failure(event, handler, error)

    def _record_failure(
        self,
        event: Event,
        handler: Handler,
        error: Exception,
    ) -> None:
        label = getattr(handler, "__qualname__", None) or repr(handler)
        LOGGER.exception(
            "subscriber %s failed on %s event from %s",
            label,
            event.kind.value,
            event.source,
        )
        entry: dict[str, object] = {
            "kind": event.kind.value,
            "source": event.source,
            "message": event.message,
            "handler": label,
            "error": f"{type(error).__name__}: {error}",
            "at": utcnow().isoformat(),
        }
        with self._lock:
            self._failed += 1
            self._failures.append(entry)
