"""Read-only projections of the line for the console."""

from __future__ import annotations

from collections.abc import Callable

from ..domain.events import Event
from ..domain.kinds import STREAM_KINDS


class ViewOps:
    """Mixed into :class:`~linecontrol.line.machine.Line`."""

    def subscribe(
        self, handler: Callable[[Event], None], *, replay: int = 0
    ) -> Callable[[], None]:
        """Subscribe ``handler`` to every kind forwarded to the console.

        With ``replay`` > 0 the handler is first backfilled with up to that
        many recently published events, in publication order, so a console
        attaching mid-run can reconcile what it missed.  Returns a callable
        that unsubscribes from every kind at once.
        """

        return self.bus.subscribe_all(STREAM_KINDS, handler, replay=replay)

    def close(self) -> None:
        """Release the line's own bus subscriptions."""

        unsubscribe = self._audit_unsubscribe
        self._audit_unsubscribe = None
        if unsubscribe is not None:
            unsubscribe()

    def sequence_snapshot(self) -> dict[str, object]:
        return self.seq.snapshot()

    def clear_sequence(self) -> None:
        self.seq.clear()

    def sequence_failures(self) -> list[dict[str, object]]:
        return [step.to_dict() for step in self.seq.failures()]

    def state(self) -> dict[str, object]:
        confirm = self.entry.confirm()
        rinse, _ = self.pickle.rinse_persisted()
        voltage, current = self.weld.power_state()
        return {
            "phase": self.phase.value,
            "coilNo": self.coil_no,
            "handle": self.handle,
            "coil": self.entry.coil_id(),
            "head": self.entry.head_position(),
            "aligned": confirm.ok,
            "position": confirm.position,
            "centerError": self.entry.center_error(),
            "correction": self.entry.correction(),
            "sampleCount": self.entry.sample_count(),
            "sampleAvg": self.entry.moving_average(),
            "sampleSettle": self.entry.sample_settle(),
            "weld": self.weld.snapshot(),
            "tension": self.tension.snapshot(),
            "pickle": {
                "level": self.pickle.level(),
                "minLevel": self.pickle.minimum_level(),
                "rinsing": self.pickle.rinsing(),
                "offset": self.pickle.offset(),
                "concentration": self.pickle.concentration(),
                "flushed": self.pickle.flushed(),
                "trend": self.pickle.level_trend(),
                "rinsingPersist": rinse.active,
            },
            "pump": self.pump.snapshot(),
            "dry": self.dry.snapshot(),
            "exit": self.exit.snapshot(),
            "strip": self.strip.snapshot(),
            "alarms": self.audit.active_alarms(),
            "alarmActive": self.audit.alarm_active("weld-overcurrent"),
            "sequence": self.seq.snapshot(),
        }

    def run_report(self) -> dict[str, object]:
        last_weld, has_weld = self.weld.last_weld()
        backups = self.store.backup_files()
        pump_state, pump_ok = self.pump.persisted()
        return {
            "phase": self.phase.value,
            "coil": self.entry.coil_id(),
            "coilNo": self.coil_no,
            "handle": self.handle,
            "lastSeam": self.weld.seam_id(),
            "track": self.strip.track_position(),
            "travel": self.strip.travel(),
            "tension": self.tension.setpoint(),
            "gain": self.tension.gain(),
            "effectiveTension": self.tension.data.effective_tension(),
            "pumpRunSec": self.pump.run_seconds(),
            "pumpHoursPersisted": self.pump.persisted_hours(),
            "pumpPersist": pump_state.running,
            "pumpPersistOk": pump_ok,
            "pumpStarted": (
                self.pump.started_at().isoformat() if self.pump.started_at() else None
            ),
            "weldKnown": has_weld,
            "lastWeldPos": last_weld.position,
            "heatInput": last_weld.heat_input,
            "backupCount": len(backups),
            "lastBackup": self.last_backup.to_dict() if self.last_backup else None,
            "dataDir": self.store.directory,
            "sequence": self.seq.snapshot(),
            "failures": self.sequence_failures(),
            "bus": {"delivered": self.bus.delivered()},
        }
