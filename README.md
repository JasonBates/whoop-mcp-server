# WHOOP MCP Server

An MCP (Model Context Protocol) server that provides Claude Desktop with access to your WHOOP fitness tracker data, including recovery scores, sleep metrics, and strain data.

## Features

- **Recovery Data**: Get your daily recovery score, HRV, resting heart rate, and SpO2
- **Sleep Analysis**: View sleep duration, stages (light/deep/REM), efficiency, and performance
- **Recovery Trends**: Track your recovery over the past 7-14 days
- **Strain Metrics**: Monitor daily strain, calories, and heart rate data
- **Workout History**: View recent workouts with sport type, strain, calories, and HR zones

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- A WHOOP membership and device
- WHOOP Developer account (free)

## Setup

### 1. Clone and Install

```bash
git clone https://github.com/JasonBates/whoop-mcp-server.git
cd whoop-mcp-server
uv sync
```

### 2. Create WHOOP Developer App

1. Go to [developer.whoop.com](https://developer.whoop.com)
2. Sign in with your WHOOP account
3. Create a new application:
   - **App Name**: "WHOOP MCP" (or your preference)
   - **Redirect URI**: `http://localhost:8080/callback`
4. Note your **Client ID** and **Client Secret**

### 3. Configure Credentials

Create a `.env` file from the example:

```bash
cp .env.example .env
```

Edit `.env` and add your credentials:

```
WHOOP_CLIENT_ID=your_client_id_here
WHOOP_CLIENT_SECRET=your_client_secret_here
```

### 4. Authorize with WHOOP

Run the token acquisition script:

```bash
uv run python scripts/get_tokens.py
```

This will:
- Open your browser to log in to WHOOP
- Request authorization for the app
- Save your access and refresh tokens to `.env`
- Test the API connection

### 5. Configure Claude Desktop

Add the server to your Claude Desktop config at:
`~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "whoop": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/whoop-mcp-server",
        "run",
        "python",
        "-m",
        "whoop_mcp"
      ]
    }
  }
}
```

Replace `/path/to/whoop-mcp-server` with the full path where you cloned the repository.

### 6. Restart Claude Desktop

Quit and reopen Claude Desktop. You should see "whoop" in the MCP servers list.

## Usage

Once configured, you can ask Claude things like:

- "What's my WHOOP status today?" (uses the combined summary)
- "How did I sleep last night?"
- "Show me my recovery trend for the past week"
- "What's my current strain?"
- "Show me my recent workouts"

## Available Tools

| Tool | Description |
|------|-------------|
| `get_today_summary` | Today's recovery, sleep, and strain in one call |
| `get_sleep_trend` | Sleep history (default 7 days, unlimited) |
| `get_recovery_trend` | Recovery history (default 7 days, unlimited) |
| `get_workouts` | Workout history (default 5, unlimited) |

## Troubleshooting

### "Authentication error: No access token found"

Run the token script: `uv run python scripts/get_tokens.py`

### Tokens expired

The server automatically refreshes tokens, but if you encounter persistent auth errors, re-run the token script.

### Multiple machines

Each machine must authorize separately with WHOOP. You cannot copy `.env` tokens between machines because when one machine refreshes the token (which happens automatically every ~55 minutes), it invalidates the token on the other machine. Run `uv run python scripts/get_tokens.py` on each machine you want to use.

### Rate limiting

WHOOP API allows 100 requests/minute and 10,000/day. The MCP server caches data appropriately to avoid hitting limits.

## Development

```bash
# Test with MCP Inspector
npx @modelcontextprotocol/inspector uv run python -m whoop_mcp
```

## License

MIT
