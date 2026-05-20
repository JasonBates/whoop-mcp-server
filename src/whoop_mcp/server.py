"""
WHOOP MCP Server - Expose WHOOP health metrics to Claude Desktop.

This MCP (Model Context Protocol) server provides tools for Claude to access
WHOOP fitness tracker data including recovery scores, sleep analysis, strain
metrics, and workout history.

Available tools:
    - get_today_summary: Daily snapshot of recovery, sleep, and strain
    - get_sleep_trend: Multi-day sleep duration and quality analysis
    - get_recovery_trend: Multi-day recovery and HRV patterns
    - get_workouts: Recent workout details with HR zones

Usage:
    Run via MCP: uv run whoop-mcp
    Configure in Claude Desktop's claude_desktop_config.json
"""

import asyncio
from datetime import datetime

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from whoop_mcp.client import WhoopClient, WhoopAuthError, WhoopAPIError

# Initialize the FastMCP server with a display name shown in Claude Desktop
mcp = FastMCP("WHOOP Recovery")


def _format_date(dt: datetime) -> str:
    """Format a datetime as 'Day D Mon' e.g. 'Fri 7 Mar'."""
    return dt.strftime("%a %-d %b")


def _format_sleep_range(start: datetime, end: datetime) -> str:
    """Format a sleep window e.g. 'Thu 6 Mar 22:47 → Fri 7 Mar 06:12'."""
    return f"{start.strftime('%a %-d %b %H:%M')} → {end.strftime('%a %-d %b %H:%M')}"


def format_hours_minutes(hours: float) -> str:
    """Convert decimal hours to human-readable 'Xh Ym' format.

    Args:
        hours: Time in decimal hours (e.g., 7.5 for 7 hours 30 minutes)

    Returns:
        Formatted string like "7h 30m"
    """
    h = int(hours)
    m = int((hours - h) * 60)
    return f"{h}h {m}m"


@mcp.tool()
async def get_today_summary() -> str:
    """Get today's complete WHOOP status: recovery, sleep, and strain in one call.

    This is the recommended daily check-in tool. Returns:
    - Recovery score with HRV and resting heart rate
    - Last night's sleep duration and quality
    - Current strain level and calories burned
    """
    try:
        client = WhoopClient()

        # Ensure token is fresh BEFORE concurrent calls to avoid race condition
        # where all 3 requests try to refresh simultaneously
        await client.ensure_fresh_token()

        # Fetch recovery, sleep, and strain data concurrently for efficiency
        # Using asyncio.gather() reduces total wait time vs sequential calls
        recovery_task = client.get_today_recovery()
        sleep_task = client.get_last_sleep()
        cycles_task = client.get_cycles(limit=1)  # Most recent cycle = today's strain

        recovery, sleep, cycles = await asyncio.gather(
            recovery_task, sleep_task, cycles_task
        )

        lines = ["=== WHOOP Daily Summary ===", ""]

        # Recovery section
        # score_state can be: SCORED (data ready), PENDING_SCORE (processing),
        # or UNSCORABLE (not enough data, e.g., WHOOP wasn't worn)
        recovery_date = _format_date(recovery.created_at) if recovery else None
        lines.append(f"RECOVERY (scored: {recovery_date})" if recovery_date else "RECOVERY")
        if recovery and recovery.score_state == "SCORED" and recovery.score:
            score = recovery.score
            lines.append(f"  Score: {score.recovery_score}%")
            lines.append(f"  HRV: {score.hrv_rmssd_milli:.1f}ms")
            lines.append(f"  Resting HR: {score.resting_heart_rate}bpm")
            if score.spo2_percentage:
                lines.append(f"  SpO2: {score.spo2_percentage:.1f}%")
        elif recovery and recovery.score_state != "SCORED":
            lines.append(f"  {recovery.score_state.lower().replace('_', ' ')}")
        else:
            lines.append("  Not available yet")

        lines.append("")

        # Sleep section
        sleep_range = _format_sleep_range(sleep.start, sleep.end) if sleep else None
        lines.append(f"SLEEP ({sleep_range})" if sleep_range else "SLEEP")
        if sleep and sleep.score_state == "SCORED" and sleep.score:
            score = sleep.score
            stages = score.stage_summary
            total = format_hours_minutes(stages.total_sleep_hours)
            deep = format_hours_minutes(stages.deep_sleep_hours)
            rem = format_hours_minutes(stages.rem_sleep_hours)
            lines.append(f"  Total: {total}")
            lines.append(f"  Deep: {deep} | REM: {rem}")
            if score.sleep_performance_percentage:
                lines.append(f"  Performance: {score.sleep_performance_percentage:.0f}%")
        elif sleep and sleep.score_state != "SCORED":
            lines.append(f"  {sleep.score_state.lower().replace('_', ' ')}")
        else:
            lines.append("  Not available yet")

        lines.append("")

        # Strain section
        cycle = cycles[0] if cycles else None
        cycle_date = _format_date(cycle.start) if cycle else None
        lines.append(f"STRAIN (cycle started: {cycle_date})" if cycle_date else "STRAIN")
        if cycle and cycle.score_state == "SCORED" and cycle.score:
            score = cycle.score
            calories = int(score.kilojoule * 0.239)
            lines.append(f"  Score: {score.strain:.1f} / 21")
            lines.append(f"  Calories: {calories} kcal")
            lines.append(f"  Avg HR: {score.average_heart_rate}bpm")
        elif cycle and cycle.score_state != "SCORED":
            lines.append(f"  {cycle.score_state.lower().replace('_', ' ')}")
        else:
            lines.append("  Not available yet")

        return "\n".join(lines)

    except (WhoopAuthError, WhoopAPIError) as e:
        raise ToolError(str(e))


@mcp.tool()
async def get_sleep_trend(days: int = 7) -> str:
    """Get sleep data for the last N days.

    Args:
        days: Number of days to look back (default: 7, no limit)

    Shows sleep duration, efficiency, and performance trends.
    """

    try:
        client = WhoopClient()
        records = await client.get_sleep(limit=days)

        if not records:
            return "No sleep data available."

        # Filter to main sleeps only (no naps)
        main_sleeps = [r for r in records if not r.nap]

        if not main_sleeps:
            return "No main sleep data available."

        lines = [f"Sleep Trend (last {len(main_sleeps)} nights):", ""]

        for record in reversed(main_sleeps):
            if record.score_state == "SCORED" and record.score:
                stages = record.score.stage_summary
                hours = stages.total_sleep_hours
                perf = record.score.sleep_performance_percentage or 0
                date = record.end.strftime("%m/%d")

                # Visual bar based on hours (8h = full bar)
                filled = min(int(hours * 10 / 8), 10)
                bar = "█" * filled + "░" * (10 - filled)
                lines.append(f"{date}: {bar} {hours:.1f}h ({perf:.0f}% perf)")
            else:
                date = record.end.strftime("%m/%d")
                lines.append(f"{date}: [not scored]")

        # Calculate averages
        scored = [r for r in main_sleeps if r.score_state == "SCORED" and r.score]
        if scored:
            avg_hours = sum(r.score.stage_summary.total_sleep_hours for r in scored) / len(scored)
            avg_perf = sum(r.score.sleep_performance_percentage or 0 for r in scored) / len(scored)
            avg_deep = sum(r.score.stage_summary.deep_sleep_hours for r in scored) / len(scored)
            lines.append("")
            lines.append(f"Average: {avg_hours:.1f}h sleep, {avg_perf:.0f}% performance, {avg_deep:.1f}h deep")

        return "\n".join(lines)

    except (WhoopAuthError, WhoopAPIError) as e:
        raise ToolError(str(e))


@mcp.tool()
async def get_recovery_trend(days: int = 7) -> str:
    """Get recovery scores for the last N days.

    Args:
        days: Number of days to look back (default: 7, no limit)

    Shows the trend of your recovery to help identify patterns.
    """

    try:
        client = WhoopClient()
        records = await client.get_recovery_trend(days)

        if not records:
            return "No recovery data available."

        lines = [f"Recovery Trend (last {len(records)} days):", ""]

        for record in reversed(records):
            if record.score_state == "SCORED" and record.score:
                score = record.score.recovery_score
                hrv = record.score.hrv_rmssd_milli
                date = record.created_at.strftime("%m/%d")

                # Simple visualization
                filled = int(score) // 10
                bar = "█" * filled + "░" * (10 - filled)
                lines.append(f"{date}: {bar} {score:.0f}% (HRV: {hrv:.0f}ms)")
            else:
                date = record.created_at.strftime("%m/%d")
                lines.append(f"{date}: [not scored]")

        # Calculate averages
        scored = [r for r in records if r.score_state == "SCORED" and r.score]
        if scored:
            avg_recovery = sum(r.score.recovery_score for r in scored) / len(scored)
            avg_hrv = sum(r.score.hrv_rmssd_milli for r in scored) / len(scored)
            lines.append("")
            lines.append(f"Average: {avg_recovery:.0f}% recovery, {avg_hrv:.0f}ms HRV")

        return "\n".join(lines)

    except (WhoopAuthError, WhoopAPIError) as e:
        raise ToolError(str(e))


@mcp.tool()
async def get_workouts(limit: int = 5) -> str:
    """Get recent workouts with strain and heart rate data.

    Args:
        limit: Number of workouts to return (default: 5, no limit)

    Shows your recent activities including sport type, strain,
    duration, calories, and heart rate zones.
    """

    try:
        client = WhoopClient()
        workouts = await client.get_workouts(limit=limit)

        if not workouts:
            return "No workout data available."

        lines = [f"Recent Workouts ({len(workouts)}):", ""]

        for w in workouts:
            # Calculate duration
            duration_mins = (w.end - w.start).total_seconds() / 60
            date = w.start.strftime("%m/%d %H:%M")
            sport = w.sport_name.replace("_", " ").title()

            if w.score_state == "SCORED" and w.score:
                s = w.score
                workout_line = f"• {date} - {sport} ({duration_mins:.0f}min)"
                lines.append(workout_line)
                lines.append(f"  ID: {w.id}")
                lines.append(f"  Strain: {s.strain:.1f} | {s.calories} cal | Avg HR: {s.average_heart_rate} bpm")

                # Show distance if available
                if s.distance_miles:
                    lines.append(f"  Distance: {s.distance_miles:.2f} mi")

                # Show HR zones summary (just the active ones)
                zones = s.zone_durations
                zone_parts = []
                if zones.zone_three_milli > 0:
                    zone_parts.append(f"Z3: {zones.zone_minutes(3):.0f}m")
                if zones.zone_four_milli > 0:
                    zone_parts.append(f"Z4: {zones.zone_minutes(4):.0f}m")
                if zones.zone_five_milli > 0:
                    zone_parts.append(f"Z5: {zones.zone_minutes(5):.0f}m")
                if zone_parts:
                    lines.append(f"  Zones: {' | '.join(zone_parts)}")

                lines.append("")
            else:
                lines.append(f"• {date} - {sport} ({duration_mins:.0f}min) [not scored]")
                lines.append(f"  ID: {w.id}")
                lines.append("")

        return "\n".join(lines).strip()

    except (WhoopAuthError, WhoopAPIError) as e:
        raise ToolError(str(e))


# Entry point for running the server
def main():
    """Run the MCP server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
