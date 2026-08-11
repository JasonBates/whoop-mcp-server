"""Tests for WHOOP MCP server tool functions."""

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastmcp.exceptions import ToolError

from whoop_mcp.server import format_hours_minutes, get_today_summary, get_sleep_trend, get_recovery_trend, get_workouts, main
from whoop_mcp.models import (
    Recovery,
    RecoveryScore,
    Sleep,
    SleepScore,
    SleepStageSummary,
    SleepNeeded,
    Cycle,
    CycleScore,
    Workout,
    WorkoutScore,
    ZoneDurations,
)
from whoop_mcp.client import WhoopAuthError, WhoopAPIError


# --- format_hours_minutes ---

class TestFormatHoursMinutes:
    def test_whole_hours(self):
        assert format_hours_minutes(7.0) == "7h 0m"

    def test_half_hour(self):
        assert format_hours_minutes(7.5) == "7h 30m"

    def test_zero(self):
        assert format_hours_minutes(0.0) == "0h 0m"

    def test_fractional(self):
        assert format_hours_minutes(1.75) == "1h 45m"


# --- Helpers to build test data ---

def _make_recovery(score_pct=85.0, hrv=65.0, rhr=55.0, state="SCORED", spo2=None):
    score = RecoveryScore(
        recovery_score=score_pct,
        resting_heart_rate=rhr,
        hrv_rmssd_milli=hrv,
        spo2_percentage=spo2,
    ) if state == "SCORED" else None
    return Recovery(
        cycle_id=1,
        user_id=1,
        created_at="2025-01-01T08:00:00Z",
        updated_at="2025-01-01T08:00:00Z",
        score_state=state,
        score=score,
    )


def _make_sleep(hours=7.5, perf=90.0, nap=False, state="SCORED"):
    ms_per_hour = 3600 * 1000
    stages = SleepStageSummary(
        total_in_bed_time_milli=int(hours * ms_per_hour * 1.1),
        total_awake_time_milli=int(hours * ms_per_hour * 0.1),
        total_light_sleep_time_milli=int(hours * ms_per_hour * 0.4),
        total_slow_wave_sleep_time_milli=int(hours * ms_per_hour * 0.3),
        total_rem_sleep_time_milli=int(hours * ms_per_hour * 0.3),
        sleep_cycle_count=4,
        disturbance_count=2,
    )
    score = SleepScore(
        stage_summary=stages,
        sleep_needed=SleepNeeded(
            baseline_milli=int(8 * ms_per_hour),
            need_from_sleep_debt_milli=0,
            need_from_recent_strain_milli=0,
        ),
        sleep_performance_percentage=perf,
    ) if state == "SCORED" else None
    return Sleep(
        id="sleep-1",
        user_id=1,
        created_at="2025-01-01T22:00:00Z",
        updated_at="2025-01-02T06:00:00Z",
        start="2025-01-01T22:00:00Z",
        end="2025-01-02T06:00:00Z",
        timezone_offset="+00:00",
        nap=nap,
        score_state=state,
        score=score,
    )


def _make_cycle(strain=12.5, kj=8000.0, avg_hr=70, max_hr=155, state="SCORED"):
    score = CycleScore(
        strain=strain,
        kilojoule=kj,
        average_heart_rate=avg_hr,
        max_heart_rate=max_hr,
    ) if state == "SCORED" else None
    return Cycle(
        id=1,
        user_id=1,
        created_at="2025-01-01T00:00:00Z",
        updated_at="2025-01-01T12:00:00Z",
        start="2025-01-01T00:00:00Z",
        timezone_offset="+00:00",
        score_state=state,
        score=score,
    )


def _make_workout(sport="pickleball", strain=14.2, state="SCORED"):
    score = WorkoutScore(
        strain=strain,
        average_heart_rate=145,
        max_heart_rate=175,
        kilojoule=2500.0,
        percent_recorded=98.0,
        distance_meter=5000.0,
        zone_durations=ZoneDurations(
            zone_zero_milli=60000,
            zone_one_milli=120000,
            zone_two_milli=300000,
            zone_three_milli=600000,
            zone_four_milli=180000,
            zone_five_milli=0,
        ),
    ) if state == "SCORED" else None
    return Workout(
        id="w-1",
        user_id=1,
        created_at="2025-01-01T10:00:00Z",
        updated_at="2025-01-01T11:00:00Z",
        start="2025-01-01T10:00:00Z",
        end="2025-01-01T11:00:00Z",
        timezone_offset="+00:00",
        sport_name=sport,
        score_state=state,
        score=score,
    )


# --- get_today_summary ---

class TestGetTodaySummary:
    @patch("whoop_mcp.server.WhoopClient")
    def test_full_summary(self, MockClient):
        mock = MockClient.return_value
        mock.ensure_fresh_token = AsyncMock()
        mock.get_today_recovery = AsyncMock(return_value=_make_recovery(spo2=98.5))
        mock.get_last_sleep = AsyncMock(return_value=_make_sleep())
        mock.get_cycles = AsyncMock(return_value=[_make_cycle()])

        result = asyncio.run(get_today_summary.fn())

        assert "WHOOP Daily Summary" in result
        assert "85" in result  # recovery score
        assert "RECOVERY" in result
        assert "SLEEP" in result
        assert "STRAIN" in result
        assert "SpO2" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_pending_recovery(self, MockClient):
        mock = MockClient.return_value
        mock.ensure_fresh_token = AsyncMock()
        mock.get_today_recovery = AsyncMock(return_value=_make_recovery(state="PENDING_SCORE"))
        mock.get_last_sleep = AsyncMock(return_value=None)
        mock.get_cycles = AsyncMock(return_value=[])

        result = asyncio.run(get_today_summary.fn())
        assert "pending score" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_no_data(self, MockClient):
        mock = MockClient.return_value
        mock.ensure_fresh_token = AsyncMock()
        mock.get_today_recovery = AsyncMock(return_value=None)
        mock.get_last_sleep = AsyncMock(return_value=None)
        mock.get_cycles = AsyncMock(return_value=[])

        result = asyncio.run(get_today_summary.fn())
        assert "Not available yet" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_auth_error(self, MockClient):
        MockClient.side_effect = WhoopAuthError("bad token")

        # server.py raises ToolError rather than returning a string, so a
        # failed source surfaces as a FAILED context source in alix instead of
        # silently becoming the agent's WHOOP data. The message must survive.
        with pytest.raises(ToolError, match="bad token"):
            asyncio.run(get_today_summary.fn())

    @patch("whoop_mcp.server.WhoopClient")
    def test_api_error(self, MockClient):
        mock = MockClient.return_value
        mock.ensure_fresh_token = AsyncMock(side_effect=WhoopAPIError("server down"))

        # server.py raises ToolError rather than returning a string, so a
        # failed source surfaces as a FAILED context source in alix instead of
        # silently becoming the agent's WHOOP data. The message must survive.
        with pytest.raises(ToolError, match="server down"):
            asyncio.run(get_today_summary.fn())


# --- get_sleep_trend ---

class TestGetSleepTrend:
    @patch("whoop_mcp.server.WhoopClient")
    def test_sleep_trend(self, MockClient):
        mock = MockClient.return_value
        mock.get_sleep = AsyncMock(return_value=[_make_sleep(hours=7.0), _make_sleep(hours=8.0)])

        result = asyncio.run(get_sleep_trend.fn(days=7))
        assert "Sleep Trend" in result
        assert "█" in result
        assert "Average" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_no_sleep_data(self, MockClient):
        mock = MockClient.return_value
        mock.get_sleep = AsyncMock(return_value=[])

        result = asyncio.run(get_sleep_trend.fn(days=7))
        assert "No sleep data" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_naps_filtered(self, MockClient):
        mock = MockClient.return_value
        mock.get_sleep = AsyncMock(return_value=[_make_sleep(nap=True)])

        result = asyncio.run(get_sleep_trend.fn(days=7))
        assert "No main sleep" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_unscored_sleep(self, MockClient):
        mock = MockClient.return_value
        mock.get_sleep = AsyncMock(return_value=[_make_sleep(state="PENDING_SCORE")])

        result = asyncio.run(get_sleep_trend.fn(days=7))
        assert "not scored" in result


# --- get_recovery_trend ---

class TestGetRecoveryTrend:
    @patch("whoop_mcp.server.WhoopClient")
    def test_recovery_trend(self, MockClient):
        mock = MockClient.return_value
        mock.get_recovery_trend = AsyncMock(return_value=[_make_recovery(score_pct=85.0), _make_recovery(score_pct=70.0)])

        result = asyncio.run(get_recovery_trend.fn(days=7))
        assert "Recovery Trend" in result
        assert "█" in result
        assert "Average" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_no_recovery_data(self, MockClient):
        mock = MockClient.return_value
        mock.get_recovery_trend = AsyncMock(return_value=[])

        result = asyncio.run(get_recovery_trend.fn(days=7))
        assert "No recovery data" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_unscored_recovery(self, MockClient):
        mock = MockClient.return_value
        mock.get_recovery_trend = AsyncMock(return_value=[_make_recovery(state="UNSCORABLE")])

        result = asyncio.run(get_recovery_trend.fn(days=7))
        assert "not scored" in result


# --- get_workouts ---

class TestGetWorkouts:
    @patch("whoop_mcp.server.WhoopClient")
    def test_workouts(self, MockClient):
        mock = MockClient.return_value
        mock.get_workouts = AsyncMock(return_value=[_make_workout()])

        result = asyncio.run(get_workouts.fn(limit=5))
        assert "Recent Workouts" in result
        assert "Pickleball" in result
        assert "Strain" in result
        assert "Distance" in result
        assert "Z3" in result
        # Workout UUID is exposed so webhook-driven agents (Alix workout-log)
        # can match the triggering event's resource_id to a workout row.
        assert "ID: w-1" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_no_workouts(self, MockClient):
        mock = MockClient.return_value
        mock.get_workouts = AsyncMock(return_value=[])

        result = asyncio.run(get_workouts.fn(limit=5))
        assert "No workout data" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_unscored_workout(self, MockClient):
        mock = MockClient.return_value
        mock.get_workouts = AsyncMock(return_value=[_make_workout(state="PENDING_SCORE")])

        result = asyncio.run(get_workouts.fn(limit=5))
        assert "not scored" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_auth_error(self, MockClient):
        MockClient.side_effect = WhoopAuthError("expired")

        # server.py raises ToolError rather than returning a string, so a
        # failed source surfaces as a FAILED context source in alix instead of
        # silently becoming the agent's WHOOP data. The message must survive.
        with pytest.raises(ToolError, match="expired"):
            asyncio.run(get_workouts.fn(limit=5))

    @patch("whoop_mcp.server.WhoopClient")
    def test_api_error(self, MockClient):
        mock = MockClient.return_value
        mock.get_workouts = AsyncMock(side_effect=WhoopAPIError("timeout"))

        # server.py raises ToolError rather than returning a string, so a
        # failed source surfaces as a FAILED context source in alix instead of
        # silently becoming the agent's WHOOP data. The message must survive.
        with pytest.raises(ToolError, match="timeout"):
            asyncio.run(get_workouts.fn(limit=5))

    @patch("whoop_mcp.server.WhoopClient")
    def test_workout_with_zone_five(self, MockClient):
        """Workouts with zone 5 activity show Z5 in output."""
        w = _make_workout()
        w.score.zone_durations.zone_five_milli = 120000  # 2 minutes in Z5
        mock = MockClient.return_value
        mock.get_workouts = AsyncMock(return_value=[w])

        result = asyncio.run(get_workouts.fn(limit=5))
        assert "Z5" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_workout_no_distance(self, MockClient):
        """Workouts without distance don't show distance line."""
        w = _make_workout()
        w.score.distance_meter = None
        mock = MockClient.return_value
        mock.get_workouts = AsyncMock(return_value=[w])

        result = asyncio.run(get_workouts.fn(limit=5))
        assert "Distance" not in result


# --- get_today_summary: pending sleep and strain ---

class TestGetTodaySummaryPendingStates:
    @patch("whoop_mcp.server.WhoopClient")
    def test_pending_sleep(self, MockClient):
        mock = MockClient.return_value
        mock.ensure_fresh_token = AsyncMock()
        mock.get_today_recovery = AsyncMock(return_value=_make_recovery())
        mock.get_last_sleep = AsyncMock(return_value=_make_sleep(state="PENDING_SCORE"))
        mock.get_cycles = AsyncMock(return_value=[_make_cycle()])

        result = asyncio.run(get_today_summary.fn())
        assert "pending score" in result

    @patch("whoop_mcp.server.WhoopClient")
    def test_pending_strain(self, MockClient):
        mock = MockClient.return_value
        mock.ensure_fresh_token = AsyncMock()
        mock.get_today_recovery = AsyncMock(return_value=_make_recovery())
        mock.get_last_sleep = AsyncMock(return_value=_make_sleep())
        mock.get_cycles = AsyncMock(return_value=[_make_cycle(state="PENDING_SCORE")])

        result = asyncio.run(get_today_summary.fn())
        assert "pending score" in result


# --- get_sleep_trend: error handlers ---

class TestGetSleepTrendErrors:
    @patch("whoop_mcp.server.WhoopClient")
    def test_auth_error(self, MockClient):
        MockClient.side_effect = WhoopAuthError("bad token")

        # server.py raises ToolError rather than returning a string, so a
        # failed source surfaces as a FAILED context source in alix instead of
        # silently becoming the agent's WHOOP data. The message must survive.
        with pytest.raises(ToolError, match="bad token"):
            asyncio.run(get_sleep_trend.fn(days=7))

    @patch("whoop_mcp.server.WhoopClient")
    def test_api_error(self, MockClient):
        mock = MockClient.return_value
        mock.get_sleep = AsyncMock(side_effect=WhoopAPIError("server error"))

        # server.py raises ToolError rather than returning a string, so a
        # failed source surfaces as a FAILED context source in alix instead of
        # silently becoming the agent's WHOOP data. The message must survive.
        with pytest.raises(ToolError, match="server error"):
            asyncio.run(get_sleep_trend.fn(days=7))


# --- get_recovery_trend: error handlers ---

class TestGetRecoveryTrendErrors:
    @patch("whoop_mcp.server.WhoopClient")
    def test_auth_error(self, MockClient):
        MockClient.side_effect = WhoopAuthError("bad token")

        # server.py raises ToolError rather than returning a string, so a
        # failed source surfaces as a FAILED context source in alix instead of
        # silently becoming the agent's WHOOP data. The message must survive.
        with pytest.raises(ToolError, match="bad token"):
            asyncio.run(get_recovery_trend.fn(days=7))

    @patch("whoop_mcp.server.WhoopClient")
    def test_api_error(self, MockClient):
        mock = MockClient.return_value
        mock.get_recovery_trend = AsyncMock(side_effect=WhoopAPIError("server error"))

        # server.py raises ToolError rather than returning a string, so a
        # failed source surfaces as a FAILED context source in alix instead of
        # silently becoming the agent's WHOOP data. The message must survive.
        with pytest.raises(ToolError, match="server error"):
            asyncio.run(get_recovery_trend.fn(days=7))


# --- main() and __main__ ---

class TestMain:
    @patch("whoop_mcp.server.mcp")
    def test_main_calls_run(self, mock_mcp):
        main()
        # show_banner=False: FastMCP's 20-line ASCII banner went to stderr on
        # every start, which Alix logs as [whoop:stderr] — 1151 times, drowning
        # the token diagnostics that share that channel.
        mock_mcp.run.assert_called_once_with(transport="stdio", show_banner=False)

    def test_dunder_main_imports(self):
        """Importing __main__ module should work without error."""
        import whoop_mcp.__main__  # noqa: F401


class TestCleanShutdown:
    """A normal stop must not look like a crash.

    Alix stops a stdio backend by closing stdin / SIGTERM; anyio raises
    KeyboardInterrupt and Python printed a ~15-line traceback to stderr.
    Alix captures stderr as "[whoop:stderr]", so 632 ordinary restarts between
    2026-07-28 and 08-11 were logged looking exactly like failures — in the
    same channel as the token-lineage diagnostics.
    """

    def test_keyboardinterrupt_exits_quietly(self, capsys):
        with patch("whoop_mcp.server.mcp.run", side_effect=KeyboardInterrupt):
            main()  # must not raise
        err = capsys.readouterr().err
        assert "clean exit" in err
        assert "Traceback" not in err
        assert "KeyboardInterrupt" not in err

    def test_real_errors_still_propagate(self):
        """Only the shutdown path is swallowed — a genuine failure must surface."""
        with patch("whoop_mcp.server.mcp.run", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                main()
