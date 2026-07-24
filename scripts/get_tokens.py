#!/usr/bin/env python3
"""
WHOOP OAuth Token Acquisition Script

This script handles the OAuth 2.0 authorization code flow to obtain
access and refresh tokens from WHOOP's API.

Usage:
    1. Set WHOOP_CLIENT_ID and WHOOP_CLIENT_SECRET in .env
    2. Run: uv run python scripts/get_tokens.py
    3. Browser will open for WHOOP login
    4. After authorization, tokens are saved to .env
"""

import os
import sys
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlencode, parse_qs, urlparse
from pathlib import Path

import httpx
from dotenv import load_dotenv, set_key

def resolve_env_file() -> Path:
    """Resolve the dotenv file to read creds from and write tokens to.

    Honors $WHOOP_TOKEN_FILE so a dedicated consumer (e.g. the Alix harness)
    can authorize into its own token file, keeping its rotating refresh-token
    lineage separate from other processes that spawn this server. Mirrors
    find_env_file() in whoop_mcp.client. Unset → the repo-root .env.
    """
    override = os.getenv("WHOOP_TOKEN_FILE")
    if override and override.strip():
        return Path(override).expanduser()
    return Path(__file__).parent.parent / ".env"


# Load existing .env (or the dedicated token file, if WHOOP_TOKEN_FILE is set)
ENV_PATH = resolve_env_file()
load_dotenv(ENV_PATH)

# WHOOP OAuth Configuration
WHOOP_AUTH_URL = "https://api.prod.whoop.com/oauth/oauth2/auth"
WHOOP_TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"
REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = "read:recovery read:sleep read:cycles read:workout read:profile offline"


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Handle the OAuth callback from WHOOP."""

    authorization_code = None

    def do_GET(self):
        """Handle GET request with authorization code."""
        parsed = urlparse(self.path)

        if parsed.path == "/callback":
            query_params = parse_qs(parsed.query)

            if "code" in query_params:
                OAuthCallbackHandler.authorization_code = query_params["code"][0]
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"""
                    <html>
                    <head><title>WHOOP Authorization Success</title></head>
                    <body style="font-family: sans-serif; text-align: center; padding: 50px;">
                        <h1>Authorization Successful!</h1>
                        <p>You can close this window and return to the terminal.</p>
                    </body>
                    </html>
                """)
            else:
                error = query_params.get("error", ["Unknown error"])[0]
                self.send_response(400)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(f"""
                    <html>
                    <head><title>Authorization Failed</title></head>
                    <body style="font-family: sans-serif; text-align: center; padding: 50px;">
                        <h1>Authorization Failed</h1>
                        <p>Error: {error}</p>
                    </body>
                    </html>
                """.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def get_authorization_url(client_id: str) -> str:
    """Build the WHOOP authorization URL."""
    params = {
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPES,
        "state": "whoop_mcp_auth",
    }
    return f"{WHOOP_AUTH_URL}?{urlencode(params)}"


def exchange_code_for_tokens(client_id: str, client_secret: str, code: str) -> dict:
    """Exchange authorization code for access and refresh tokens."""
    data = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": REDIRECT_URI,
        "code": code,
    }

    response = httpx.post(WHOOP_TOKEN_URL, data=data)
    response.raise_for_status()
    return response.json()


def save_tokens_to_env(access_token: str, refresh_token: str):
    """Save tokens to .env file."""
    # Create .env if it doesn't exist
    if not ENV_PATH.exists():
        ENV_PATH.touch()

    # quote_mode="never" prevents quotes that can break token parsing
    set_key(str(ENV_PATH), "WHOOP_ACCESS_TOKEN", access_token, quote_mode="never")
    set_key(str(ENV_PATH), "WHOOP_REFRESH_TOKEN", refresh_token, quote_mode="never")
    print(f"\n✓ Tokens saved to {ENV_PATH}")


def test_api_connection(access_token: str):
    """Test the API connection by fetching recovery data."""
    print("\nTesting API connection...")

    headers = {"Authorization": f"Bearer {access_token}"}
    response = httpx.get(
        "https://api.prod.whoop.com/developer/v2/recovery",
        headers=headers,
        params={"limit": 1}
    )

    if response.status_code == 200:
        data = response.json()
        if data.get("records"):
            record = data["records"][0]
            score = record.get("score", {})
            recovery = score.get("recovery_score", "N/A")
            hrv = score.get("hrv_rmssd_milli", "N/A")
            rhr = score.get("resting_heart_rate", "N/A")
            print(f"✓ API connection successful!")
            print(f"  Latest Recovery: {recovery}%")
            print(f"  HRV: {hrv:.1f}ms" if isinstance(hrv, (int, float)) else f"  HRV: {hrv}")
            print(f"  RHR: {rhr}bpm")
        else:
            print("✓ API connection successful (no recovery data yet)")
    else:
        print(f"✗ API test failed: {response.status_code}")
        print(f"  Response: {response.text}")


def main():
    """Run the OAuth flow."""
    print("=" * 50)
    print("WHOOP OAuth Token Acquisition")
    print("=" * 50)

    # Check for credentials
    client_id = os.getenv("WHOOP_CLIENT_ID")
    client_secret = os.getenv("WHOOP_CLIENT_SECRET")

    if not client_id or not client_secret or client_id == "your_client_id_here":
        print("\n✗ Missing WHOOP credentials!")
        print("\nTo get started:")
        print("1. Go to https://developer.whoop.com")
        print("2. Create an application")
        print("3. Copy your Client ID and Client Secret")
        print(f"4. Add them to {ENV_PATH}")
        print("\nExample .env content:")
        print("  WHOOP_CLIENT_ID=abc123...")
        print("  WHOOP_CLIENT_SECRET=xyz789...")
        sys.exit(1)

    print(f"\nClient ID: {client_id[:8]}...")
    print(f"Redirect URI: {REDIRECT_URI}")
    print(f"Scopes: {SCOPES}")

    # Start local server
    server = HTTPServer(("localhost", 8080), OAuthCallbackHandler)
    print("\n→ Starting local callback server on port 8080...")

    # Open browser for authorization
    auth_url = get_authorization_url(client_id)
    print("→ Opening browser for WHOOP authorization...")
    webbrowser.open(auth_url)

    print("\nWaiting for authorization callback...")
    print("(Complete the login in your browser)")

    # Wait for callback
    while OAuthCallbackHandler.authorization_code is None:
        server.handle_request()

    server.server_close()
    code = OAuthCallbackHandler.authorization_code
    print(f"\n✓ Received authorization code: {code[:10]}...")

    # Exchange code for tokens
    print("\n→ Exchanging code for tokens...")
    try:
        tokens = exchange_code_for_tokens(client_id, client_secret, code)
        access_token = tokens["access_token"]
        refresh_token = tokens.get("refresh_token", "")

        print(f"✓ Access token received: {access_token[:20]}...")
        if refresh_token:
            print(f"✓ Refresh token received: {refresh_token[:20]}...")
        else:
            print("⚠ No refresh token received (did you include 'offline' scope?)")

        # Save tokens
        save_tokens_to_env(access_token, refresh_token)

        # Test the connection
        test_api_connection(access_token)

        print("\n" + "=" * 50)
        print("Setup complete! You can now use the WHOOP MCP server.")
        print("=" * 50)

    except httpx.HTTPStatusError as e:
        print(f"\n✗ Token exchange failed: {e.response.status_code}")
        print(f"  Response: {e.response.text}")
        sys.exit(1)


if __name__ == "__main__":
    main()
