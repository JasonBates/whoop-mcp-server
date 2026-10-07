# WHOOP MCP Server

A FastMCP server exposing WHOOP data (recovery, sleep, strain, workouts) to Claude Desktop. Tools are the `@mcp.tool` functions in `src/whoop_mcp/server.py`; `client.py` is the async API client (`https://api.prod.whoop.com/developer`) with OAuth refresh; `models.py` holds the Pydantic models.

**Status (7 October 2026):** not registered in Claude Code (`~/.claude.json`) or Claude Desktop config; the WHOOP server in use is `whoop-ts` from `~/CODE/repos/whoop-mcp-ts`.

## Commands

```bash
uv run python scripts/get_tokens.py   # first time: OAuth tokens into the token file
uv run whoop-mcp                      # run the server (stdio)
uv run pytest                         # tests/ (client, models, server)
```

## Tokens

- Token file: `$WHOOP_TOKEN_FILE` if set, else `.env` in the project root, else `./.env` (gitignored). It holds `WHOOP_CLIENT_ID`, `WHOOP_CLIENT_SECRET`, `WHOOP_ACCESS_TOKEN`, `WHOOP_REFRESH_TOKEN`; refreshed tokens are written back to it (proactively after 55 minutes).
- WHOOP rotates the refresh token on every use and invalidates the old one, so two long-lived consumers must never share a token file; give each its own via `WHOOP_TOKEN_FILE`.
- `scripts/refresh-probe.py` is a standalone harness for diagnosing dying refresh-token lineages. It refuses Alix's production token file; run it only against an isolated grant, as its docstring describes. Never log token values.
