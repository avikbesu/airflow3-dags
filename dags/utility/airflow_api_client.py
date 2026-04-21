"""
Shared Airflow 3.x REST API client helper.

Airflow 3.x replaced HTTP Basic Auth with JWT Bearer tokens on the REST API.
This module provides get_session() which exchanges credentials for a JWT and
returns a pre-configured requests.Session ready for /api/v2/ calls.

Credential resolution order
----------------------------
1. Airflow Connection  conn_id=``airflow_api``  (conn_type HTTP,
   host=<base URL>, login=<user>, password=<password>)
2. Environment variables:
     AIRFLOW_API_BASE_URL  (default: http://localhost:8080)
     AIRFLOW_API_USER      (default: admin)
     AIRFLOW_API_PASSWORD  (default: admin)

The ``override_url`` parameter lets callers substitute the base URL at
call-time (e.g. from DAG run conf), while still using stored credentials.

Token endpoint
--------------
POST {base_url}/auth/token  →  {"access_token": "<jwt>"}
(Airflow SimpleAuthManager; FAB auth manager uses the same path via the
 public router at /api/v2/auth/ which proxies to the same handler.)
"""

from __future__ import annotations

import os

_CONN_ID = "airflow_api"
_DEFAULT_API_URL = "http://localhost:8080"


def get_session(override_url: str | None = None):
    """
    Return ``(base_url, requests.Session)`` authenticated with a JWT Bearer token.

    Args:
        override_url: If provided, use this as the API base URL instead of the
                      connection/env-var value.  Credentials are still resolved
                      from the connection / env vars.
    """
    import requests

    if override_url:
        base_url = override_url.rstrip("/")
        user = os.getenv("AIRFLOW_API_USER", "admin")
        password = os.getenv("AIRFLOW_API_PASSWORD", "admin")
    else:
        try:
            from airflow.hooks.base import BaseHook
            conn = BaseHook.get_connection(_CONN_ID)
            base_url = (conn.host or _DEFAULT_API_URL).rstrip("/")
            user = conn.login or ""
            password = conn.password or ""
        except Exception:
            base_url = os.getenv("AIRFLOW_API_BASE_URL", _DEFAULT_API_URL).rstrip("/")
            user = os.getenv("AIRFLOW_API_USER", "admin")
            password = os.getenv("AIRFLOW_API_PASSWORD", "admin")

    resp = requests.post(
        f"{base_url}/auth/token",
        json={"username": user, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]

    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"
    return base_url, session
