"""Database access helpers for resume tailoring."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from db import database


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_profile(payload: dict) -> dict:
    now = _now_iso()

    profile_id = payload.get("profile_id")
    name = payload["name"].strip()
    location = payload.get("location", "").strip()
    headline = payload.get("headline", "").strip()
    constraints = json.dumps(payload.get("constraints", {}), ensure_ascii=True)

    if profile_id:
        existing = database.query("SELECT id FROM user_profile WHERE id = ?", (profile_id,))
        if existing:
            database.execute(
                """UPDATE user_profile
                   SET name = ?, location = ?, headline = ?, constraints_json = ?, updated_at = ?
                   WHERE id = ?""",
                (name, location, headline, constraints, now, profile_id),
            )
        else:
            existing_by_name = database.query(
                "SELECT id FROM user_profile WHERE name = ? LIMIT 1", (name,)
            )
            if existing_by_name:
                profile_id = existing_by_name[0]["id"]
                database.execute(
                    """UPDATE user_profile
                       SET location = ?, headline = ?, constraints_json = ?, updated_at = ?
                       WHERE id = ?""",
                    (location, headline, constraints, now, profile_id),
                )
            else:
                database.execute(
                    """INSERT INTO user_profile (name, location, headline, constraints_json, updated_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (name, location, headline, constraints, now),
                )
                profile_id = database.query(
                    "SELECT id FROM user_profile WHERE name = ? ORDER BY id DESC LIMIT 1", (name,)
                )[0]["id"]
    else:
        database.execute(
            """INSERT INTO user_profile (name, location, headline, constraints_json, updated_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                   location = excluded.location,
                   headline = excluded.headline,
                   constraints_json = excluded.constraints_json,
                   updated_at = excluded.updated_at""",
            (name, location, headline, constraints, now),
        )
        profile_id = database.query(
            "SELECT id FROM user_profile WHERE name = ? ORDER BY id DESC LIMIT 1", (name,)
        )[0]["id"]

    facts = payload.get("facts", [])
    database.execute("DELETE FROM profile_fact WHERE profile_id = ?", (profile_id,))
    for fact in facts:
        database.execute(
            """INSERT INTO profile_fact
               (profile_id, fact_type, source_section, raw_text, normalized_keywords, priority, active, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                profile_id,
                fact.get("fact_type", "experience"),
                fact.get("source_section", ""),
                fact.get("raw_text", ""),
                json.dumps(fact.get("normalized_keywords", []), ensure_ascii=True),
                int(fact.get("priority", 50)),
                1 if fact.get("active", True) else 0,
                now,
            ),
        )

    return get_profile(profile_id)


def get_profile(profile_id: int | None = None) -> dict | None:
    if profile_id:
        rows = database.query(
            "SELECT id, name, location, headline, constraints_json, updated_at FROM user_profile WHERE id = ?",
            (profile_id,),
        )
    else:
        rows = database.query(
            "SELECT id, name, location, headline, constraints_json, updated_at FROM user_profile ORDER BY updated_at DESC LIMIT 1"
        )

    if not rows:
        return None

    profile = rows[0]
    facts = database.query(
        """SELECT fact_type, source_section, raw_text, normalized_keywords, priority, active
           FROM profile_fact
           WHERE profile_id = ?
           ORDER BY priority DESC, id ASC""",
        (profile["id"],),
    )

    parsed_facts = []
    for fact in facts:
        try:
            keywords = json.loads(fact.get("normalized_keywords") or "[]")
        except Exception:
            keywords = []
        parsed_facts.append(
            {
                "fact_type": fact.get("fact_type", "other"),
                "source_section": fact.get("source_section", ""),
                "raw_text": fact.get("raw_text", ""),
                "normalized_keywords": keywords,
                "priority": fact.get("priority", 50),
                "active": bool(fact.get("active", 1)),
            }
        )

    try:
        constraints = json.loads(profile.get("constraints_json") or "{}")
    except Exception:
        constraints = {}

    return {
        "id": profile["id"],
        "name": profile.get("name", ""),
        "location": profile.get("location", ""),
        "headline": profile.get("headline", ""),
        "constraints": constraints,
        "facts": parsed_facts,
        "updated_at": profile.get("updated_at", _now_iso()),
    }


def log_generation_event(
    *,
    trace_id: str,
    status: str,
    latency_ms: int,
    model_route: dict[str, str],
    token_in: int,
    token_out: int,
    error_code: str = "",
):
    now = _now_iso()
    database.execute(
        """INSERT INTO generation_event
           (trace_id, created_at, status, latency_ms, model_route, token_in, token_out, error_code)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            trace_id,
            now,
            status,
            latency_ms,
            json.dumps(model_route, ensure_ascii=True),
            int(token_in),
            int(token_out),
            error_code,
        ),
    )


def current_daily_tokens() -> int:
    rows = database.query(
        """SELECT COALESCE(SUM(token_in + token_out), 0) as total
           FROM generation_event
           WHERE DATE(created_at) >= date('now')"""
    )
    return int(rows[0]["total"] if rows else 0)
