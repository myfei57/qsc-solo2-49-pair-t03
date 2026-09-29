"""In-process publish/subscribe bus used for cross-area reactions."""

from __future__ import annotations

import logging
import threading
from collections import deque
from collections.abc import Callable, Iterable

from .domain.events import Event
from .domain.kinds import EventKind

Handler = Callable[[Event], None]

LOGGER = logging.getLogger("linecontrol.bus")

#: Number of published events retained for late-joining subscribers.
HISTORY_LIMIT = 300


class EventBus:
    """Delivers events to subscribers registered for the matching kind.

    Handlers are copied out of the registry before dispatch so that a handler
    may unsubscribe or publish a follow-up event without disturbing the
    current dispatch pass.  Every handler runs in isolation: a subscriber
    that raises is logged and counted, but the remaining handlers still
    receive the event and the failure never propagates into the publisher.

    The bus retains a bounded window of recently published events so a
    subscriber attaching later can replay the tail it missed, e.g. a console
    reconnecting mid-run for reconciliation.
    """

    def __init__(self, history_limit: int = HISTORY_LIMIT) -> None:
        self._lock = threading.Lock()
        self._next_id = 0
        self._subs: dict[EventKind, list[tuple[int, Handler]]] = {}
        self._history: deque[Event] = deque(maxlen=history_limit)
        self._delivered = 0
        self._failed = 0

    def publish(self, event: Event) -> None:
        with self._lock:
            handlers = [handler for _, handler in self._subs.get(event.kind, ())]
            self._history.append(event)
            self._delivered += 1
        for handler in handlers:
            self._dispatch(event, handler)

    def subscribe(
        self, kind: EventKind, handler: Handler, *, replay: int = 0
    ) -> Callable[[], None]:
        """Register ``handler`` for ``kind``; return an unsubscribe callable.

        With ``replay`` > 0 the handler is first backfilled with up to that
        many retained events of this kind, oldest first.  Registration and
        the backlog snapshot happen under one lock hold, so nothing published
        before the call is missed; events published concurrently from other
        threads may still arrive before the backlog finishes replaying.
        """

        with self._lock:
            self._next_id += 1
            token = self._next_id
            self._subs.setdefault(kind, []).append((token, handler))
            backlog = self._backlog_locked((kind,), replay)
        self._replay(backlog, handler)

        def unsubscribe() -> None:
            with self._lock:
                entries = self._subs.get(kind, [])
                self._subs[kind] = [item for item in entries if item[0] != token]

        return unsubscribe

    def subscribe_all(
        self, kinds: Iterable[EventKind], handler: Handler, *, replay: int = 0
    ) -> Callable[[], None]:
        """Register ``handler`` for every kind in ``kinds`` in one step.

        Behaves like :meth:`subscribe` per kind, but the replay backlog is
        merged across the kinds and delivered in publication order, and the
        returned callable unsubscribes from all of them at once.
        """

        wanted = tuple(kinds)
        with self._lock:
            tokens = []
            for kind in wanted:
                self._next_id += 1
                tokens.append((kind, self._next_id))
                self._subs.setdefault(kind, []).append((self._next_id, handler))
            backlog = self._backlog_locked(wanted, replay)
        self._replay(backlog, handler)

        def unsubscribe() -> None:
            with self._lock:
                for kind, token in tokens:
                    entries = self._subs.get(kind, [])
                    self._subs[kind] = [item for item in entries if item[0] != token]

        return unsubscribe

    def recent(
        self, kind: EventKind | None = None, limit: int | None = None
    ) -> list[Event]:
        """Return retained events, oldest first, optionally filtered by ``kind``."""

        with self._lock:
            events = [
                event for event in self._history if kind is None or event.kind is kind
            ]
        if limit is not None:
            events = events[-limit:] if limit > 0 else []
        return events

    def subscribers(self, kind: EventKind) -> int:
        with self._lock:
            return len(self._subs.get(kind, ()))

    def delivered(self) -> int:
        with self._lock:
            return self._delivered

    def failed(self) -> int:
        """Number of subscriber invocations that raised."""

        with self._lock:
            return self._failed

    def _backlog_locked(
        self, kinds: tuple[EventKind, ...], replay: int
    ) -> list[Event]:
        if replay <= 0:
            return []
        wanted = set(kinds)
        events = [event for event in self._history if event.kind in wanted]
        return events[-replay:]

    def _replay(self, backlog: list[Event], handler: Handler) -> None:
        for event in backlog:
            self._dispatch(event, handler)

    def _dispatch(self, event: Event, handler: Handler) -> None:
        try:
            handler(event)
        except Exception:
            with self._lock:
                self._failed += 1
            LOGGER.exception("subscriber %r raised on %s event", handler, event.kind)
