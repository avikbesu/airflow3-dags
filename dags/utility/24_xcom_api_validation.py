"""
DAG: 24_xcom_api_validation

Full XCom round-trip validation for Airflow 3, exercising all four push/pull paths:

  ┌─────────────┬────────────────────────────────────────────────────────────────┐
  │ Path        │ Covered by                                                     │
  ├─────────────┼────────────────────────────────────────────────────────────────┤
  │ SDK → SDK   │ sdk_multi_push ──► sdk_consume_injected (via extra_sdk_key)    │
  │ SDK → API   │ sdk_multi_push ──► api_list_entries + api_read_each_key        │
  │ API → SDK   │ api_inject_external ──► sdk_consume_injected                  │
  │ API → API   │ api_inject_external ──► api_verify_injected                   │
  └─────────────┴────────────────────────────────────────────────────────────────┘

Also validates:
  - GET  .../xcomEntries              (list all entries for a task — no key suffix)
  - GET  .../xcomEntries/{key}        (read a specific entry with ?deserialize=true)
  - POST .../xcomEntries              (inject a new entry mid-DAG via the API)
  - Type and value fidelity for str / int / float / list / dict payloads
  - External-system injection pattern: API writes, SDK reads

Task flow:
    sdk_multi_push ──► api_list_entries ──► api_read_each_key ──► validation_report
    api_inject_external ──► sdk_consume_injected ──────────────────────►┘
                        └──► api_verify_injected ──────────────────────►┘

Run config (optional):
    { "api_base_url": "http://webserver:8080" }

Auth: Airflow Connection `airflow_api` (conn_type=HTTP) or env vars
      AIRFLOW_API_BASE_URL / AIRFLOW_API_USER / AIRFLOW_API_PASSWORD.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task

from utility.airflow_api_client import get_session

# Five typed payloads pushed by sdk_multi_push.  Used by api_read_each_key
# for type-and-value assertions after the REST round-trip.
_TYPED_PAYLOADS: dict[str, object] = {
    "str_val":   "hello airflow 3",
    "int_val":   42,
    "float_val": 3.14,
    "list_val":  [1, "two", 3.0, True],
    "dict_val":  {"framework": "airflow", "version": 3, "nested": {"ok": True}},
}

# Key pushed explicitly by sdk_multi_push for SDK→SDK verification
_SDK_EXTRA_KEY = "extra_sdk_key"
_SDK_EXTRA_VALUE = {"note": "pushed via SDK explicit key"}


def _ctx_xcom_base(ctx) -> tuple[str, str, str, str]:
    """Return (base_url, dag_id, run_id, conf) from task context + api client."""
    dag_run = ctx["dag_run"]
    conf = dag_run.conf or {}
    base_url, session = get_session(conf.get("api_base_url") or None)
    return base_url, ctx["dag"].dag_id, dag_run.run_id, session


@dag(
    dag_id="24_xcom_api_validation",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=compose", "exec=kube",
        "subtype=xcom",
        "intent=demo",
    ],
    doc_md=__doc__,
    params={"api_base_url": ""},
)
def xcom_api_validation():

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 1 — SDK push
    # ──────────────────────────────────────────────────────────────────────────

    @task(task_id="sdk_multi_push")
    def sdk_multi_push(ti=None) -> dict:
        """
        Push 5 typed XCom entries (str/int/float/list/dict) via ti.xcom_push,
        plus one explicit extra key for the SDK→SDK path.
        Return value is auto-pushed as `return_value`.
        """
        for key, value in _TYPED_PAYLOADS.items():
            ti.xcom_push(key=key, value=value)
        ti.xcom_push(key=_SDK_EXTRA_KEY, value=_SDK_EXTRA_VALUE)

        total = len(_TYPED_PAYLOADS) + 1  # typed payloads + extra_sdk_key
        print(f"[sdk_multi_push] pushed {total} named entries + return_value")
        return {"pushed_keys": list(_TYPED_PAYLOADS.keys()) + [_SDK_EXTRA_KEY], "count": total}

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 2 — API: list all entries for sdk_multi_push
    # ──────────────────────────────────────────────────────────────────────────

    @task(task_id="api_list_entries")
    def api_list_entries() -> dict:
        """
        GET .../taskInstances/sdk_multi_push/xcomEntries (no key suffix).
        Validates that every pushed key is present in the listing response.

        This exercises the LIST endpoint, which DAG 11 does not cover.
        """
        ctx = get_current_context()
        base_url, dag_id, run_id, session = _ctx_xcom_base(ctx)

        url = (
            f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/sdk_multi_push/xcomEntries"
        )
        r = session.get(url, params={"limit": 50}, timeout=30)
        r.raise_for_status()
        data = r.json()

        entries = data.get("xcom_entries", [])
        found_keys = {e["key"] for e in entries}
        expected_keys = set(_TYPED_PAYLOADS.keys()) | {_SDK_EXTRA_KEY, "return_value"}

        missing = expected_keys - found_keys
        if missing:
            raise AirflowFailException(f"[api_list_entries] Missing keys in listing: {missing}")

        print(
            f"[api_list_entries] total_entries={data.get('total_entries')}  "
            f"found_keys={sorted(found_keys)}"
        )
        return {
            "total_entries": data.get("total_entries"),
            "found_keys": sorted(found_keys),
            "check_passed": True,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 3 — API: read each typed key individually
    # ──────────────────────────────────────────────────────────────────────────

    @task(task_id="api_read_each_key")
    def api_read_each_key() -> dict:
        """
        GET .../xcomEntries/{key}?deserialize=true for each typed key.
        Asserts both type fidelity (str/int/float/list/dict survive the
        Airflow 3 JSON serialisation round-trip) and value equality.
        """
        ctx = get_current_context()
        base_url, dag_id, run_id, session = _ctx_xcom_base(ctx)

        results: dict[str, dict] = {}
        failures: list[str] = []

        for key, expected in _TYPED_PAYLOADS.items():
            url = (
                f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
                f"/taskInstances/sdk_multi_push/xcomEntries/{key}"
            )
            r = session.get(url, params={"deserialize": "true"}, timeout=30)
            r.raise_for_status()
            payload = r.json()

            # Airflow 3 API returns the XCom entry object; the stored value is at "value"
            actual = payload.get("value", payload)

            # bool is a subclass of int in Python; use exact type comparison
            type_ok = type(actual) is type(expected)
            value_ok = actual == expected

            results[key] = {
                "expected_type": type(expected).__name__,
                "actual_type":   type(actual).__name__,
                "type_match":    type_ok,
                "value_match":   value_ok,
            }
            status = "PASS" if (type_ok and value_ok) else "FAIL"
            print(f"[api_read_each_key] {key:12s} {status}  actual={actual!r}")

            if not (type_ok and value_ok):
                failures.append(key)

        if failures:
            raise AirflowFailException(
                f"[api_read_each_key] Type/value assertion failures: {failures}\n"
                f"Detail: {json.dumps(results, indent=2)}"
            )

        return {"validation_results": results, "all_passed": True}

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 4 — API: inject a new XCom entry mid-DAG (external-system pattern)
    # ──────────────────────────────────────────────────────────────────────────

    @task(task_id="api_inject_external")
    def api_inject_external() -> dict:
        """
        POST a new XCom entry onto THIS task instance via the REST API.

        This simulates an external system (e.g. a data pipeline, a webhook
        handler, or a CI job) injecting data into a running DAG without
        touching Airflow internals.  The injected key is then consumed by
        both the SDK path and the API verify path.
        """
        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        dag_id = ctx["dag"].dag_id
        run_id = dag_run.run_id
        ti = ctx["ti"]
        conf = dag_run.conf or {}

        base_url, session = get_session(conf.get("api_base_url") or None)
        url = (
            f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{ti.task_id}/xcomEntries"
        )
        payload = {
            "source": "external_system",
            "injected_at": datetime.now(timezone.utc).isoformat(),
            "records": [{"id": i, "value": i * 100} for i in range(1, 4)],
            "meta": {"version": "v2", "tags": ["prod", "validated"]},
        }
        body = {"key": "external_payload", "value": payload}

        r = session.post(url, json=body, timeout=30)
        if r.status_code not in (200, 201):
            raise AirflowFailException(
                f"[api_inject_external] POST failed {r.status_code}: {r.text}"
            )
        print(f"[api_inject_external] Injected via API POST:\n  {json.dumps(payload, indent=2)}")
        return payload  # also stored as return_value XCom for this task

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 5a — API → SDK: pull the injected entry via Task SDK
    # ──────────────────────────────────────────────────────────────────────────

    @task(task_id="sdk_consume_injected")
    def sdk_consume_injected(ti=None) -> dict:
        """
        Pull `external_payload` from api_inject_external using ti.xcom_pull.
        Proves that an API-pushed entry is immediately visible to the SDK.
        """
        pulled = ti.xcom_pull(task_ids="api_inject_external", key="external_payload")
        if pulled is None:
            raise AirflowFailException(
                "[sdk_consume_injected] xcom_pull returned None — API→SDK interop failed"
            )

        required_keys = {"source", "injected_at", "records", "meta"}
        missing = required_keys - set(pulled.keys())
        if missing:
            raise AirflowFailException(
                f"[sdk_consume_injected] Injected payload missing keys: {missing}"
            )

        # Also verify the extra_sdk_key pushed by sdk_multi_push (SDK→SDK path)
        sdk_extra = ti.xcom_pull(task_ids="sdk_multi_push", key=_SDK_EXTRA_KEY)
        sdk_sdk_ok = sdk_extra == _SDK_EXTRA_VALUE

        print(
            f"[sdk_consume_injected] API→SDK PASS  pulled_keys={sorted(pulled.keys())}\n"
            f"[sdk_consume_injected] SDK→SDK PASS={sdk_sdk_ok}  extra={sdk_extra!r}"
        )
        return {
            "api_to_sdk_pass": True,
            "sdk_to_sdk_pass": sdk_sdk_ok,
            "pulled_keys": sorted(pulled.keys()),
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 5b — API → API: verify the injected entry via REST API GET
    # ──────────────────────────────────────────────────────────────────────────

    @task(task_id="api_verify_injected")
    def api_verify_injected() -> dict:
        """
        GET the API-injected `external_payload` back via REST API.
        Closes the API→API round-trip: POST then GET on the same entry.
        """
        ctx = get_current_context()
        base_url, dag_id, run_id, session = _ctx_xcom_base(ctx)

        url = (
            f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/api_inject_external/xcomEntries/external_payload"
        )
        r = session.get(url, params={"deserialize": "true"}, timeout=30)
        r.raise_for_status()
        data = r.json()

        actual = data.get("value", data)
        required_keys = {"source", "injected_at", "records", "meta"}
        missing = required_keys - set(actual.keys() if isinstance(actual, dict) else [])
        if missing:
            raise AirflowFailException(
                f"[api_verify_injected] GET response missing keys: {missing}  got={actual!r}"
            )
        print(
            f"[api_verify_injected] API→API PASS\n"
            f"  source={actual.get('source')}  "
            f"  records_count={len(actual.get('records', []))}"
        )
        return {"api_to_api_pass": True, "retrieved_keys": sorted(actual.keys())}

    # ──────────────────────────────────────────────────────────────────────────
    # Phase 6 — Final validation report
    # ──────────────────────────────────────────────────────────────────────────

    @task(task_id="validation_report")
    def validation_report(ti=None) -> dict:
        """
        Collect XCom from all validation tasks, print a pass/fail matrix,
        and raise AirflowFailException if any check failed.
        """
        list_result     = ti.xcom_pull(task_ids="api_list_entries")
        read_result     = ti.xcom_pull(task_ids="api_read_each_key")
        consume_result  = ti.xcom_pull(task_ids="sdk_consume_injected")
        verify_result   = ti.xcom_pull(task_ids="api_verify_injected")

        checks = {
            "sdk_push → api_list_all_entries":     (list_result or {}).get("check_passed", False),
            "sdk_push → api_read_per_key (types)": (read_result or {}).get("all_passed", False),
            "api_push → sdk_pull":                 (consume_result or {}).get("api_to_sdk_pass", False),
            "sdk_push → sdk_pull (extra key)":     (consume_result or {}).get("sdk_to_sdk_pass", False),
            "api_push → api_get (round-trip)":     (verify_result or {}).get("api_to_api_pass", False),
        }

        bar = "═" * 54
        print(f"\n{bar}")
        print("  XCom Validation Report — Airflow 3 API + SDK")
        print(bar)
        for name, passed in checks.items():
            icon = "✓ PASS" if passed else "✗ FAIL"
            print(f"  {icon}  {name}")
        print(f"{bar}\n")

        failures = [name for name, passed in checks.items() if not passed]
        if failures:
            raise AirflowFailException(
                f"XCom validation failed for {len(failures)} check(s): {failures}"
            )

        return {"all_passed": True, "checks": checks}

    # ──────────────────────────────────────────────────────────────────────────
    # Wiring
    # ──────────────────────────────────────────────────────────────────────────
    pushed   = sdk_multi_push()
    listed   = api_list_entries()
    read     = api_read_each_key()
    injected = api_inject_external()
    consumed = sdk_consume_injected()
    verified = api_verify_injected()
    report   = validation_report()

    #   sdk_multi_push ──► api_list_entries ──► api_read_each_key ──► validation_report
    #   api_inject_external ──► sdk_consume_injected ───────────────────────►┘
    #                       └──► api_verify_injected ───────────────────────►┘
    pushed >> listed >> read >> report
    injected >> [consumed, verified]
    [consumed, verified] >> report


xcom_api_validation()
