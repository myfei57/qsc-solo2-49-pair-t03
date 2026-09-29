"""Process event bus behaviour."""

from __future__ import annotations

from linecontrol.bus import EventBus
from linecontrol.domain.events import Event
from linecontrol.domain.kinds import EventKind
from linecontrol.line.machine import Line


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
        raise RuntimeError("handler exploded")

    bus.subscribe(EventKind.ARC, broken)
    bus.subscribe(EventKind.ARC, lambda event: seen.append(event.message))
    bus.publish(Event(kind=EventKind.ARC, source="weld", message="struck"))
    assert seen == ["struck"]
    assert bus.delivered() == 1
    assert bus.failed() == 1
    failures = bus.failures()
    assert len(failures) == 1
    assert failures[0]["kind"] == "arc"
    assert "handler exploded" in str(failures[0]["error"])


def test_failing_handler_does_not_raise_into_the_publisher() -> None:
    bus = EventBus()

    def broken(event: Event) -> None:
        raise ValueError("bad event")

    bus.subscribe(EventKind.STOP, broken)
    bus.publish(Event(kind=EventKind.STOP, source="line", message="halt"))
    assert bus.failed() == 1


def test_late_subscriber_replays_recent_matching_events() -> None:
    bus = EventBus()
    for index in range(5):
        bus.publish(Event(kind=EventKind.TRACK, source="strip", message=f"t{index}"))
    bus.publish(Event(kind=EventKind.STOP, source="line", message="halt"))
    seen: list[str] = []
    bus.subscribe(EventKind.TRACK, lambda event: seen.append(event.message), replay=3)
    assert seen == ["t2", "t3", "t4"]
    bus.publish(Event(kind=EventKind.TRACK, source="strip", message="t5"))
    assert seen == ["t2", "t3", "t4", "t5"]


def test_subscribe_without_replay_delivers_no_backlog() -> None:
    bus = EventBus()
    bus.publish(Event(kind=EventKind.DRY, source="dry", message="heated"))
    seen: list[str] = []
    bus.subscribe(EventKind.DRY, lambda event: seen.append(event.message))
    assert seen == []


def test_recent_exposes_history_for_reconciliation() -> None:
    bus = EventBus()
    bus.publish(Event(kind=EventKind.PUMP, source="pump", message="started"))
    bus.publish(Event(kind=EventKind.LEVEL, source="tank", message="low"))
    bus.publish(Event(kind=EventKind.PUMP, source="pump", message="stopped"))
    assert [event.message for event in bus.recent()] == ["started", "low", "stopped"]
    pumps = bus.recent(EventKind.PUMP)
    assert [event.message for event in pumps] == ["started", "stopped"]
    assert [event.message for event in bus.recent(limit=2)] == ["low", "stopped"]


def test_line_audit_survives_a_failing_subscriber(line: Line) -> None:
    def broken(event: Event) -> None:
        raise RuntimeError("console stream died")

    line.bus.subscribe(EventKind.AUDIT, broken)
    line.bus.publish(Event(kind=EventKind.AUDIT, source="test", message="checkpoint"))
    messages = [event.message for event in line.audit_query(EventKind.AUDIT, 5)]
    assert "checkpoint" in messages
    assert line.bus.failed() == 1
