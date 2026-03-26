"""Databricks transcript import source."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from inspect_ai.model import ChatMessage, ChatMessageAssistant, ChatMessageUser

from inspect_scout._transcript.types import Transcript

from .client import connect_databricks_sql, resolve_databricks_config

DATABRICKS_SOURCE_TYPE = "databricks"


@dataclass(frozen=True)
class DatabricksRow:
    """Typed container for a DB row mapped by column names."""

    values: dict[str, Any]

    def get_str(self, key: str) -> str | None:
        value = self.values.get(key)
        if value is None:
            return None
        return str(value)

    def get_int(self, key: str) -> int | None:
        value = self.values.get(key)
        if value is None:
            return None
        return int(value)


def _qualified_table(
    table: str,
    catalog: str | None,
    schema: str | None,
) -> str:
    if "." in table:
        return table
    if catalog and schema:
        return f"{catalog}.{schema}.{table}"
    if schema:
        return f"{schema}.{table}"
    return table


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _row_to_dict(cursor: Any, row: Any) -> dict[str, Any]:
    columns = [d[0] for d in cursor.description]
    if isinstance(row, dict):
        return cast(dict[str, Any], row)
    return dict(zip(columns, row, strict=False))


def _build_messages(turn_rows: list[DatabricksRow]) -> list[ChatMessage]:
    messages: list[ChatMessage] = []
    for turn in turn_rows:
        question = turn.get_str("agent_question")
        answer = turn.get_str("participant_response_text")
        if question:
            messages.append(ChatMessageUser(content=question))
        if answer:
            messages.append(ChatMessageAssistant(content=answer))
    return messages


def _build_transcript(
    transcript_row: DatabricksRow,
    turn_rows: list[DatabricksRow],
) -> Transcript:
    transcript_id = transcript_row.get_str("transcript_id")
    if not transcript_id:
        raise ValueError("Databricks row missing transcript_id.")

    metadata = {
        "created_at": _to_iso(transcript_row.values.get("created_at")),
        "updated_at": _to_iso(transcript_row.values.get("updated_at")),
        "deleted_at": _to_iso(transcript_row.values.get("deleted_at")),
        "survey_id": transcript_row.get_str("survey_id"),
        "participant_survey_question_id": transcript_row.get_str(
            "participant_survey_question_id"
        ),
    }
    metadata = {k: v for k, v in metadata.items() if v is not None}

    messages = _build_messages(turn_rows)

    return Transcript(
        transcript_id=transcript_id,
        source_type=DATABRICKS_SOURCE_TYPE,
        source_id=transcript_id,
        source_uri=transcript_row.get_str("source_uri"),
        date=_to_iso(transcript_row.values.get("updated_at"))
        or _to_iso(transcript_row.values.get("created_at")),
        message_count=len(messages),
        messages=messages,
        metadata=metadata,
    )


def _query_transcripts_sql(
    transcript_table: str,
    survey_question_table: str,
    survey_table: str,
    survey_id: str | None,
    from_time: datetime | None,
    to_time: datetime | None,
    include_deleted: bool,
    limit: int | None,
) -> tuple[str, list[Any]]:
    where: list[str] = []
    params: list[Any] = []

    if survey_id is not None:
        where.append("ps.survey_id = %s")
        params.append(survey_id)
    if from_time is not None:
        where.append("coalesce(prc.updated_at, prc.created_at) >= %s")
        params.append(from_time)
    if to_time is not None:
        where.append("coalesce(prc.updated_at, prc.created_at) < %s")
        params.append(to_time)
    if not include_deleted:
        where.append("prc.deleted_at IS NULL")

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    limit_sql = "LIMIT %s" if limit is not None else ""
    if limit is not None:
        params.append(limit)

    sql = f"""
        SELECT
            CAST(prc.id AS STRING) AS transcript_id,
            prc.created_at,
            prc.updated_at,
            prc.deleted_at,
            CAST(psq.id AS STRING) AS participant_survey_question_id,
            CAST(ps.survey_id AS STRING) AS survey_id
        FROM {transcript_table} prc
        LEFT JOIN {survey_question_table} psq
            ON prc.id = psq.id
        LEFT JOIN {survey_table} ps
            ON psq.participant_survey_id = ps.id
        {where_sql}
        ORDER BY coalesce(prc.updated_at, prc.created_at) DESC
        {limit_sql}
    """.strip()

    return sql, params


def _query_turns_sql(
    turns_table: str,
    transcript_ids: Sequence[str],
    include_deleted: bool,
) -> tuple[str, list[Any]]:
    placeholders = ", ".join(["%s"] * len(transcript_ids))
    where_deleted = "" if include_deleted else "AND deleted_at IS NULL"

    sql = f"""
        SELECT
            CAST(participant_response_conversational_id AS STRING) AS transcript_id,
            turn_index,
            agent_question,
            participant_response_text,
            started_at,
            completed_at,
            created_at,
            updated_at,
            deleted_at
        FROM {turns_table}
        WHERE participant_response_conversational_id IN ({placeholders})
        {where_deleted}
        ORDER BY participant_response_conversational_id, turn_index
    """.strip()
    return sql, list(transcript_ids)


async def databricks(
    catalog: str | None = None,
    schema: str | None = None,
    transcript_table: str = "participantresponseconversational",
    turns_table: str = "participantresponseconversationalturn",
    participant_survey_question_table: str = "participantsurveyquestion",
    participant_survey_table: str = "participantsurvey",
    survey_id: str | None = None,
    from_time: datetime | None = None,
    to_time: datetime | None = None,
    limit: int | None = None,
    include_deleted: bool = False,
    server_hostname: str | None = None,
    http_path: str | None = None,
    access_token: str | None = None,
) -> AsyncIterator[Transcript]:
    """Read transcripts from Databricks SQL tables.

    Uses Databricks SQL connector and maps normalized parent/turn rows into Scout
    ``Transcript`` records.
    """
    config = resolve_databricks_config(server_hostname, http_path, access_token)

    qualified_transcript_table = _qualified_table(transcript_table, catalog, schema)
    qualified_turns_table = _qualified_table(turns_table, catalog, schema)
    qualified_psq_table = _qualified_table(
        participant_survey_question_table, catalog, schema
    )
    qualified_ps_table = _qualified_table(participant_survey_table, catalog, schema)

    transcripts_sql, transcript_params = _query_transcripts_sql(
        transcript_table=qualified_transcript_table,
        survey_question_table=qualified_psq_table,
        survey_table=qualified_ps_table,
        survey_id=survey_id,
        from_time=from_time,
        to_time=to_time,
        include_deleted=include_deleted,
        limit=limit,
    )

    rows = await asyncio.to_thread(
        _fetch_rows,
        config.server_hostname,
        config.http_path,
        config.access_token,
        transcripts_sql,
        transcript_params,
    )

    transcript_rows = [DatabricksRow(values=row) for row in rows]
    if not transcript_rows:
        return

    transcript_ids = [row.get_str("transcript_id") for row in transcript_rows]
    normalized_ids = [tid for tid in transcript_ids if tid is not None]
    if not normalized_ids:
        return

    turns_sql, turn_params = _query_turns_sql(
        turns_table=qualified_turns_table,
        transcript_ids=normalized_ids,
        include_deleted=include_deleted,
    )
    turn_rows_raw = await asyncio.to_thread(
        _fetch_rows,
        config.server_hostname,
        config.http_path,
        config.access_token,
        turns_sql,
        turn_params,
    )

    turns_by_transcript: dict[str, list[DatabricksRow]] = {}
    for turn in turn_rows_raw:
        row = DatabricksRow(values=turn)
        transcript_id = row.get_str("transcript_id")
        if transcript_id is None:
            continue
        turns_by_transcript.setdefault(transcript_id, []).append(row)

    for row in transcript_rows:
        transcript_id = row.get_str("transcript_id")
        if transcript_id is None:
            continue
        yield _build_transcript(row, turns_by_transcript.get(transcript_id, []))


def _fetch_rows(
    server_hostname: str,
    http_path: str,
    access_token: str,
    statement: str,
    params: Sequence[Any],
) -> list[dict[str, Any]]:
    conn = connect_databricks_sql(
        resolve_databricks_config(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )
    )
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(statement, params)
            rows = cursor.fetchall()
            return [_row_to_dict(cursor, row) for row in rows]
        finally:
            cursor.close()
    finally:
        conn.close()
