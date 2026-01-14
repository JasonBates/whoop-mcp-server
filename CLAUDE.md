# WHOOP MCP Server

## Overview
A FastMCP server that exposes WHOOP fitness data (recovery, sleep, strain, workouts) to Claude Desktop via the Model Context Protocol.

## Architecture

```
whoop-mcp/
├── src/whoop_mcp/
│   ├── server.py      # FastMCP server with 4 tools
│   ├── client.py      # Async WHOOP API client with OAuth token management
│   └── models.py      # Pydantic models for type-safe API responses
├── scripts/
│   └── get_tokens.py  # OAuth 2.0 token acquisition script
├── .env               # Secrets (gitignored) - tokens stored here
└── pyproject.toml     # Dependencies managed by uv
```

## Key Components

### server.py - MCP Tools
- `get_today_summary()` - Daily recovery, sleep, strain snapshot
- `get_sleep_trend(days)` - Historical sleep data with trends
- `get_recovery_trend(days)` - Historical recovery/HRV patterns
- `get_workouts(limit)` - Recent workout details with HR zones

### client.py - API Client
- Handles OAuth token refresh automatically (55-minute proactive refresh)
- Tokens persisted back to `.env` on refresh
- Base URL: `https://api.prod.whoop.com/developer`

### models.py - Data Models
- `Recovery`, `Sleep`, `Cycle`, `Workout` Pydantic models
- Helper properties for data transformation (e.g., `total_sleep_hours`)

## Secrets Management
- All secrets in `.env` file (gitignored)
- Required variables:
  - `WHOOP_CLIENT_ID`
  - `WHOOP_CLIENT_SECRET`
  - `WHOOP_ACCESS_TOKEN`
  - `WHOOP_REFRESH_TOKEN`

## Running
```bash
# First time: Get OAuth tokens
uv run python scripts/get_tokens.py

# Run server
uv run whoop-mcp
```

## Dependencies
- `fastmcp>=2.0.0` - MCP framework
- `httpx` - Async HTTP client
- `pydantic>=2.0` - Data validation
- `python-dotenv` - Environment variable loading
