"""Tests for WHOOP Pydantic models and their computed properties."""

from datetime import datetime
from whoop_mcp.models import (
    CycleScore,
    Cycle,
    Recovery,
    RecoveryScore,
    Sleep,
    SleepNeeded,
    SleepScore,
    SleepStageSummary,
    Workout,
    WorkoutScore,
    ZoneDurations,
)


# --- SleepStageSummary computed properties ---


class TestSleepStageSummary:
    def _make(self, light=0, deep=0, rem=0, awake=0):
        return SleepStageSummary(
            total_in_bed_time_milli=light + deep + rem + awake,
            total_awake_time_milli=awake,
            total_light_sleep_time_milli=light,
            total_slow_wave_sleep_time_milli=deep,
            total_rem_sleep_time_milli=rem,
            sleep_cycle_count=3,
            disturbance_count=1,
        )

    def test_total_sleep_milli(self):
        s = self._make(light=1000, deep=2000, rem=3000)
        assert s.total_sleep_milli == 6000

    def test_total_sleep_milli_excludes_awake(self):
        s = self._make(light=1000, deep=2000, rem=3000, awake=5000)
        assert s.total_sleep_milli == 6000

    def test_total_sleep_hours(self):
        # 7.2 hours = 7.2 * 3600 * 1000 ms
        ms = int(7.2 * 3600 * 1000)
        s = self._make(light=ms)
        assert abs(s.total_sleep_hours - 7.2) < 0.001

    def test_deep_sleep_hours(self):
        ms = int(1.5 * 3600 * 1000)
        s = self._make(deep=ms)
        assert abs(s.deep_sleep_hours - 1.5) < 0.001

    def test_rem_sleep_hours(self):
        ms = int(2.0 * 3600 * 1000)
        s = self._make(rem=ms)
        assert abs(s.rem_sleep_hours - 2.0) < 0.001

    def test_light_sleep_hours(self):
        ms = int(3.0 * 3600 * 1000)
        s = self._make(light=ms)
        assert abs(s.light_sleep_hours - 3.0) < 0.001

    def test_zero_sleep(self):
        s = self._make()
        assert s.total_sleep_hours == 0.0
        assert s.deep_sleep_hours == 0.0


# --- ZoneDurations ---


class TestZoneDurations:
    def _make(self, **kwargs):
        defaults = {f"zone_{i}_milli": 0 for i in ["zero", "one", "two", "three", "four", "five"]}
        defaults.update(kwargs)
        return ZoneDurations(**defaults)

    def test_zone_minutes(self):
        z = self._make(zone_three_milli=120_000)  # 2 minutes
        assert z.zone_minutes(3) == 2.0

    def test_zone_minutes_all_zones(self):
        z = self._make(
            zone_zero_milli=60_000,
            zone_one_milli=120_000,
            zone_two_milli=180_000,
            zone_three_milli=240_000,
            zone_four_milli=300_000,
            zone_five_milli=360_000,
        )
        assert z.zone_minutes(0) == 1.0
        assert z.zone_minutes(1) == 2.0
        assert z.zone_minutes(2) == 3.0
        assert z.zone_minutes(3) == 4.0
        assert z.zone_minutes(4) == 5.0
        assert z.zone_minutes(5) == 6.0

    def test_zone_minutes_zero(self):
        z = self._make()
        assert z.zone_minutes(0) == 0.0


# --- WorkoutScore ---


class TestWorkoutScore:
    def _make(self, kilojoule=1000.0, distance_meter=None):
        return WorkoutScore(
            strain=10.5,
            average_heart_rate=150,
            max_heart_rate=180,
            kilojoule=kilojoule,
            percent_recorded=100.0,
            distance_meter=distance_meter,
            zone_durations=ZoneDurations(
                zone_zero_milli=0,
                zone_one_milli=0,
                zone_two_milli=0,
                zone_three_milli=0,
                zone_four_milli=0,
                zone_five_milli=0,
            ),
        )

    def test_calories(self):
        ws = self._make(kilojoule=1000.0)
        assert ws.calories == 239  # 1000 * 0.239

    def test_calories_rounding(self):
        ws = self._make(kilojoule=500.0)
        assert ws.calories == int(500.0 * 0.239)

    def test_distance_miles(self):
        ws = self._make(distance_meter=1609.34)
        assert abs(ws.distance_miles - 1.0) < 0.01

    def test_distance_miles_none(self):
        ws = self._make(distance_meter=None)
        assert ws.distance_miles is None

    def test_distance_miles_zero(self):
        ws = self._make(distance_meter=0)
        assert ws.distance_miles is None  # 0 is falsy


# --- Model validation from raw API data ---


class TestModelValidation:
    def test_recovery_score_from_dict(self):
        data = {
            "recovery_score": 85.0,
            "resting_heart_rate": 55.0,
            "hrv_rmssd_milli": 65.2,
            "spo2_percentage": 98.5,
            "skin_temp_celsius": 33.1,
            "user_calibrating": False,
        }
        score = RecoveryScore.model_validate(data)
        assert score.recovery_score == 85.0
        assert score.hrv_rmssd_milli == 65.2

    def test_recovery_score_optional_fields(self):
        data = {
            "recovery_score": 70.0,
            "resting_heart_rate": 60.0,
            "hrv_rmssd_milli": 45.0,
        }
        score = RecoveryScore.model_validate(data)
        assert score.spo2_percentage is None
        assert score.skin_temp_celsius is None
        assert score.user_calibrating is False

    def test_recovery_from_dict(self):
        data = {
            "cycle_id": 12345,
            "user_id": 1,
            "created_at": "2025-01-01T08:00:00Z",
            "updated_at": "2025-01-01T08:00:00Z",
            "score_state": "SCORED",
            "score": {
                "recovery_score": 90.0,
                "resting_heart_rate": 50.0,
                "hrv_rmssd_milli": 70.0,
            },
        }
        r = Recovery.model_validate(data)
        assert r.cycle_id == 12345
        assert r.score_state == "SCORED"
        assert r.score.recovery_score == 90.0

    def test_sleep_from_dict(self):
        data = {
            "id": "sleep-1",
            "user_id": 1,
            "created_at": "2025-01-01T08:00:00Z",
            "updated_at": "2025-01-01T08:00:00Z",
            "start": "2025-01-01T22:00:00Z",
            "end": "2025-01-02T06:00:00Z",
            "timezone_offset": "+00:00",
            "nap": False,
            "score_state": "SCORED",
        }
        s = Sleep.model_validate(data)
        assert s.id == "sleep-1"
        assert s.nap is False
        assert s.score is None

    def test_cycle_from_dict(self):
        data = {
            "id": 99,
            "user_id": 1,
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T12:00:00Z",
            "start": "2025-01-01T00:00:00Z",
            "timezone_offset": "+00:00",
            "score_state": "SCORED",
            "score": {
                "strain": 12.5,
                "kilojoule": 8000.0,
                "average_heart_rate": 70,
                "max_heart_rate": 155,
            },
        }
        c = Cycle.model_validate(data)
        assert c.score.strain == 12.5
        assert c.end is None

    def test_workout_from_dict(self):
        data = {
            "id": "w-1",
            "user_id": 1,
            "created_at": "2025-01-01T10:00:00Z",
            "updated_at": "2025-01-01T11:00:00Z",
            "start": "2025-01-01T10:00:00Z",
            "end": "2025-01-01T11:00:00Z",
            "timezone_offset": "+00:00",
            "sport_name": "pickleball",
            "score_state": "SCORED",
            "score": {
                "strain": 14.2,
                "average_heart_rate": 145,
                "max_heart_rate": 175,
                "kilojoule": 2500.0,
                "percent_recorded": 98.0,
                "zone_durations": {
                    "zone_zero_milli": 60000,
                    "zone_one_milli": 120000,
                    "zone_two_milli": 300000,
                    "zone_three_milli": 600000,
                    "zone_four_milli": 180000,
                    "zone_five_milli": 0,
                },
            },
        }
        w = Workout.model_validate(data)
        assert w.sport_name == "pickleball"
        assert w.score.strain == 14.2
        assert w.score.zone_durations.zone_minutes(3) == 10.0
