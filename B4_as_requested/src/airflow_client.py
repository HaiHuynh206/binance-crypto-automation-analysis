"""
Session-based Airflow 2.10 client for this deployment.

Note: Basic Auth against /api/v1/dags returns 401 on this server.
Web form login + session cookie works. Do not print credentials.
"""
from __future__ import annotations

import json
import re
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener

from src.paths import ENV_FILE, ROOT


class AirflowClientError(RuntimeError):
    pass


def load_env(path: Path | None = None) -> dict[str, str]:
    path = path or ENV_FILE
    if not path.exists():
        raise AirflowClientError(f"Missing {path} — copy .env.example and fill credentials.")
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    for key in ("AIRFLOW_URL", "AIRFLOW_USER", "AIRFLOW_PASS"):
        if not env.get(key):
            raise AirflowClientError(f".env missing {key}")
    return env


class AirflowSession:
    def __init__(self, env: dict[str, str] | None = None):
        self.env = env or load_env()
        self.base = self.env["AIRFLOW_URL"].rstrip("/")
        self._opener = build_opener(HTTPCookieProcessor(CookieJar()))
        self._login()

    def _login(self) -> None:
        html = self._opener.open(
            Request(f"{self.base}/login/", headers={"User-Agent": "group4"}), timeout=30
        ).read().decode("utf-8", "replace")
        m = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', html)
        if not m:
            raise AirflowClientError("Login page has no csrf_token — auth flow changed.")
        body = urlencode(
            {
                "username": self.env["AIRFLOW_USER"],
                "password": self.env["AIRFLOW_PASS"],
                "csrf_token": m.group(1),
            }
        ).encode()
        self._opener.open(
            Request(
                f"{self.base}/login/",
                data=body,
                headers={
                    "User-Agent": "group4",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": f"{self.base}/login/",
                },
            ),
            timeout=30,
        )

    def get_json(self, path: str) -> Any:
        try:
            r = self._opener.open(
                Request(f"{self.base}{path}", headers={"Accept": "application/json"}),
                timeout=60,
            )
            return json.loads(r.read().decode())
        except HTTPError as e:
            raise AirflowClientError(f"HTTP {e.code} for {path}: {e.read()[:300]!r}") from e

    def list_dags(self, limit: int = 50) -> list[dict]:
        return self.get_json(f"/api/v1/dags?limit={limit}").get("dags", [])


def main() -> int:
    client = AirflowSession()
    version = client.get_json("/api/v1/version")
    dags = client.list_dags()
    print("Airflow", version.get("version"), "| dags:", [d["dag_id"] for d in dags])
    ours = [d for d in dags if d["dag_id"] == "b4_market_quality"]
    if not ours:
        print(
            "STOPPED: DAG b4_market_quality not on server.\n"
            "Need admin to copy group_4/dags/b4_market_quality.py -> /opt/airflow/dags/\n"
            f"(local file: {ROOT / 'dags' / 'b4_market_quality.py'})"
        )
        print("Existing DAG on server:", [d["dag_id"] for d in dags])
        return 1
    print("b4_market_quality found, paused=", ours[0].get("is_paused"))
    return 0


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(ROOT))
    raise SystemExit(main())
