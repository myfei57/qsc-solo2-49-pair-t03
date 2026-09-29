"""Process event bus behaviour."""

from __future__ import annotations

from linecontrol.bus import EventBus
from linecontrol.domain.events import Event
from linecontrol.domain.kinds import EventKind


def test_publish_only_reaches_matching_kind() -> None:
    bus = EventBus()
    seen: list[str] = []
    bus.subscribe(EventKind.ARC, lambda event: seen.append(event.message))
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="struck"))
    bus.publish(Event(kind=EventKind.STOP, source="line", message="stopped"))
    assert seen == ["struck"]
    assert bus.delivered() == 2
    assert bus.subscribers(EventKind.ARC) == 1


def test_unsubscribe_stops_delivery() -> None:
    bus = EventBus()
    seen: list[str] = []
    stop = bus.subscribe(EventKind.TRACK, lambda event: seen.append(event.message))
    bus.publish(Event(kind=EventKind.TRACK, source="strip", message="first"))
    stop()
    bus.publish(Event(kind=EventKind.TRACK, source="strip", message="second"))
    assert seen == ["first"]
    assert bus.subscribers(EventKind.TRACK) == 0


def test_handler_may_publish_a_follow_up_event() -> None:
    bus = EventBus()
    seen: list[EventKind] = []

    def follow(event: Event) -> None:
        seen.append(event.kind)
        if event.kind is EventKind.ARC:
            bus.publish(Event(kind=EventKind.WELD, source="weld", message="seam"))

    bus.subscribe(EventKind.ARC, follow)
    bus.subscribe(EventKind.WELD, follow)
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="struck"))
    assert seen == [EventKind.ARC, EventKind.WELD]


def test_failing_handler_does_not_block_other_subscribers() -> None:
    bus = EventBus()
    seen: list[str] = []

    def broken(event: Event) -> None:
        raise RuntimeError("boom")

    bus.subscribe(EventKind.ARC, broken)
    bus.subscribe(EventKind.ARC, lambda event: seen.append(event.message))
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="struck"))
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="again"))
    assert seen == ["struck", "again"]
    assert bus.delivered() == 2
    assert bus.failed() == 2


def test_late_subscriber_replays_recent_events() -> None:
    bus = EventBus()
    for index in range(5):
        bus.publish(Event(kind=EventKind.TRACK, source="strip", message=f"m{index}"))
    bus.publish(Event(kind=EventKind.STOP, source="line", message="stopped"))
    seen: list[str] = []
    bus.subscribe(EventKind.TRACK, lambda event: seen.append(event.message), replay=3)
    assert seen == ["m2", "m3", "m4"]
    bus.publish(Event(kind=EventKind.TRACK, source="strip", message="m5"))
    assert seen == ["m2", "m3", "m4", "m5"]


def test_replay_is_bounded_by_the_history_window() -> None:
    bus = EventBus(history_limit=3)
    for index in range(5):
        bus.publish(Event(kind=EventKind.TRACK, source="strip", message=f"m{index}"))
    seen: list[str] = []
    bus.subscribe(EventKind.TRACK, lambda event: seen.append(event.message), replay=10)
    assert seen == ["m2", "m3", "m4"]


def test_subscribe_without_replay_gets_no_backlog() -> None:
    bus = EventBus()
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="struck"))
    seen: list[str] = []
    bus.subscribe(EventKind.ARC, lambda event: seen.append(event.message))
    assert seen == []


def test_replay_failure_is_isolated_like_live_dispatch() -> None:
    bus = EventBus()
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="struck"))
    calls: list[str] = []

    def flaky(event: Event) -> None:
        calls.append(event.message)
        if len(calls) == 1:
            raise RuntimeError("boom")

    bus.subscribe(EventKind.ARC, flaky, replay=1)
    assert calls == ["struck"]
    assert bus.failed() == 1
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="again"))
    assert calls == ["struck", "again"]


def test_subscribe_all_replays_in_publication_order() -> None:
    bus = EventBus()
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="a1"))
    bus.publish(Event(kind=EventKind.WELD, source="weld", message="w1"))
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="a2"))
    bus.publish(Event(kind=EventKind.PUMP, source="pump", message="p1"))
    seen: list[str] = []
    unsubscribe = bus.subscribe_all(
        [EventKind.ARC, EventKind.WELD],
        lambda event: seen.append(event.message),
        replay=10,
    )
    assert seen == ["a1", "w1", "a2"]
    unsubscribe()
    assert bus.subscribers(EventKind.ARC) == 0
    assert bus.subscribers(EventKind.WELD) == 0


def test_recent_returns_the_retained_window() -> None:
    bus = EventBus()
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="a1"))
    bus.publish(Event(kind=EventKind.WELD, source="weld", message="w1"))
    assert [event.message for event in bus.recent()] == ["a1", "w1"]
    assert [event.message for event in bus.recent(EventKind.ARC)] == ["a1"]
    assert [event.message for event in bus.recent(limit=1)] == ["w1"]
