"""Browser-based login for `authenticator: externalbrowser`.

Uses the same OAuth device authorization flow as the MotherDuck CLI and DuckDB
extension, so SSO-enforcing organizations get their identity provider's login
page. The resulting token is cached in the CLI's credentials file, so
`motherduck login` and dbt share one login.
"""

import base64
import contextlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path
from threading import Lock

# TODO: register a dedicated Auth0 application for dbt-motherduck. The prototype
# reuses the MotherDuck CLI's client, whose logins yield short-lived tokens.
OAUTH_CLIENT_ID = "bmRe4wSjYj1WzwqnJ6kptnyBnwKUgMlD"
OAUTH_DEVICE_CODE_URL = "https://auth.motherduck.com/oauth/device/code"
OAUTH_TOKEN_URL = "https://auth.motherduck.com/oauth/token"
OAUTH_AUDIENCE = "https://dev--06syqlm.us.auth0.com/api/v2/"
MOTHERDUCK_API_ORIGIN = "https://api.motherduck.com"

HTTP_TIMEOUT_SECONDS = 30
LOOKUP_USER_DEADLINE_SECONDS = 60
MAX_POLL_INTERVAL_SECONDS = 15
# Treat tokens this close to expiry as expired so a run does not start with a
# token that lapses before its connections open.
EXPIRY_LEEWAY_SECONDS = 300


class BrowserAuthError(Exception):
    pass


def credentials_file_path():
    home = os.environ.get("MOTHERDUCK_HOME")
    if home:
        if not os.path.isabs(home):
            raise BrowserAuthError(
                f'MOTHERDUCK_HOME must be an absolute path, got "{home}"'
            )
        return Path(home) / "credentials.json"
    return Path.home() / ".motherduck" / "credentials.json"


def load_saved_token(path):
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(parsed, dict) or parsed.get("version") != 1:
        return None
    token = parsed.get("motherduck_token")
    if not isinstance(token, str) or not token.strip():
        return None
    return token.strip()


def save_token(path, token):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as file:
        json.dump({"version": 1, "motherduck_token": token}, file, indent=2)
        file.write("\n")
    os.replace(temporary, path)


def token_expired(token, now=None):
    """Read the JWT `exp` claim without verifying it; the endpoint verifies."""
    try:
        payload = token.split(".")[1]
        claims = json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
        expires_at = claims["exp"]
    except (IndexError, KeyError, TypeError, ValueError):
        return False
    if not isinstance(expires_at, (int, float)):
        return False
    return expires_at - EXPIRY_LEEWAY_SECONDS <= (time.time() if now is None else now)


def http_request(url, *, data=None, headers=None):
    """Return the status and decoded JSON body (or None) for a request."""
    body = None if data is None else urllib.parse.urlencode(data).encode()
    request = urllib.request.Request(url, data=body, headers=headers or {})
    if body is not None:
        request.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read()
    try:
        return status, json.loads(raw) if raw else None
    except ValueError:
        return status, None


class BrowserAuthenticator:
    """Resolves one MotherDuck token per process and shares it across threads."""

    def __init__(
        self,
        *,
        request=http_request,
        open_browser=webbrowser.open,
        sleep=time.sleep,
        clock=time.monotonic,
        output=None,
    ):
        self._request = request
        self._open_browser = open_browser
        self._sleep = sleep
        self._clock = clock
        self._output = output
        self._lock = Lock()
        self._token = None
        # Every token this process has used, so error messages can redact them.
        self.issued_tokens = set()

    def get_token(self):
        with self._lock:
            if self._token is None or token_expired(self._token):
                self._token = self._resolve_token()
                self.issued_tokens.add(self._token)
            return self._token

    def invalidate(self, token):
        """Forget a token the endpoint rejected, unless another thread replaced it."""
        with self._lock:
            if self._token != token:
                return
            self._token = None
            path = credentials_file_path()
            if load_saved_token(path) == token:
                path.unlink(missing_ok=True)

    def _resolve_token(self):
        path = credentials_file_path()
        token = load_saved_token(path)
        if token is not None and not token_expired(token):
            return token
        token = self._login()
        try:
            save_token(path, token)
        except OSError as exc:
            self._print(
                f"Could not save MotherDuck credentials to {path}: {exc}. "
                "You will be asked to log in again next time."
            )
        return token

    def _login(self):
        try:
            status, device = self._request(
                OAUTH_DEVICE_CODE_URL,
                data={
                    "client_id": OAUTH_CLIENT_ID,
                    "scope": "openid profile email",
                    "audience": OAUTH_AUDIENCE,
                },
            )
        except OSError as exc:
            raise BrowserAuthError(
                f"Could not reach MotherDuck to log in: {exc}"
            ) from None
        if (
            status != 200
            or not isinstance(device, dict)
            or not all(
                isinstance(device.get(key), str)
                for key in ("device_code", "user_code", "verification_uri_complete")
            )
        ):
            raise BrowserAuthError(
                f"MotherDuck login request failed with HTTP {status}"
            )
        url = device["verification_uri_complete"]
        self._print(
            "Log in to MotherDuck in your browser to continue.\n"
            f"  URL:  {url}\n"
            f"  Code: {device['user_code']}"
        )
        # The printed URL is enough; headless machines often have no browser.
        with contextlib.suppress(Exception):
            self._open_browser(url)
        id_token = self._poll_for_id_token(device)
        return self._exchange_for_motherduck_token(id_token)

    def _poll_for_id_token(self, device):
        interval = device.get("interval") or 5
        deadline = self._clock() + (device.get("expires_in") or 300)
        while self._clock() < deadline:
            self._sleep(interval)
            try:
                status, body = self._request(
                    OAUTH_TOKEN_URL,
                    data={
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                        "device_code": device["device_code"],
                        "client_id": OAUTH_CLIENT_ID,
                    },
                )
            except OSError:
                # Transient network errors: keep polling until the code expires.
                continue
            body = body if isinstance(body, dict) else {}
            if status == 200 and body.get("id_token"):
                return body["id_token"]
            error = body.get("error")
            if status == 429 or error == "slow_down":
                interval = min(interval + 1, MAX_POLL_INTERVAL_SECONDS)
                continue
            if error == "authorization_pending":
                continue
            raise BrowserAuthError(
                "MotherDuck login failed: "
                f"{body.get('error_description') or error or f'HTTP {status}'}"
            )
        raise BrowserAuthError("MotherDuck login timed out. Please try again.")

    def _exchange_for_motherduck_token(self, id_token):
        deadline = self._clock() + LOOKUP_USER_DEADLINE_SECONDS
        while self._clock() < deadline:
            try:
                status, body = self._request(
                    f"{MOTHERDUCK_API_ORIGIN}/mom/lookup-user",
                    headers={"Authorization": f"Bearer {id_token}"},
                )
            except OSError:
                self._sleep(0.5)
                continue
            body = body if isinstance(body, dict) else {}
            if status == 200 and isinstance(body.get("token"), str) and body["token"]:
                return body["token"]
            if status in (401, 403, 404):
                detail = body.get("message") or "account not found or access denied"
                if body.get("requiredAlternateLoginMethodDisplayName"):
                    detail += f"; log in with {body['requiredAlternateLoginMethodDisplayName']}"
                raise BrowserAuthError(f"MotherDuck login failed: {detail}")
            self._sleep(0.5)
        raise BrowserAuthError("MotherDuck token exchange timed out. Please try again.")

    def _print(self, message):
        print(message, file=self._output or sys.stderr, flush=True)
