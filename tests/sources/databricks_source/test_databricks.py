from datetime import datetime

import pytest
from inspect_scout.sources._databricks import (
    DatabricksRow,
    _build_messages,
    _build_transcript,
    _query_transcripts_sql,
)
from inspect_scout.sources._databricks.client import resolve_databricks_config


def test_resolve_databricks_config_prefers_explicit_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "workspace.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc")
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

    config = resolve_databricks_config(
        server_hostname=None,
        http_path=None,
        access_token="explicit-token",
    )

    assert config.server_hostname == "workspace.cloud.databricks.com"
    assert config.http_path == "/sql/1.0/warehouses/abc"
    assert config.access_token == "explicit-token"


def test_query_transcripts_sql_applies_filters() -> None:
    sql, params = _query_transcripts_sql(
        transcript_table="main.analytics.participantresponseconversational",
        survey_question_table="main.analytics.participantsurveyquestion",
        survey_table="main.analytics.participantsurvey",
        survey_id="survey_1",
        from_time=datetime(2026, 1, 1),
        to_time=datetime(2026, 2, 1),
        include_deleted=False,
        limit=50,
    )

    assert "ps.survey_id = %s" in sql
    assert "prc.deleted_at IS NULL" in sql
    assert "LIMIT %s" in sql
    assert params[0] == "survey_1"
    assert params[-1] == 50


def test_build_messages_and_transcript() -> None:
    turn_rows = [
        DatabricksRow(
            {
                "turn_index": 0,
                "agent_question": "How are you?",
                "participant_response_text": "Great",
            }
        ),
        DatabricksRow(
            {
                "turn_index": 1,
                "agent_question": "Need anything else?",
                "participant_response_text": "Nope",
            }
        ),
    ]

    messages = _build_messages(turn_rows)
    assert len(messages) == 4

    transcript = _build_transcript(
        DatabricksRow(
            {
                "transcript_id": "tx_1",
                "created_at": datetime(2026, 1, 1, 12, 0, 0),
                "updated_at": datetime(2026, 1, 1, 12, 5, 0),
                "participant_survey_question_id": "psq_1",
                "survey_id": "survey_1",
            }
        ),
        turn_rows,
    )

    assert transcript.transcript_id == "tx_1"
    assert transcript.source_type == "databricks"
    assert transcript.message_count == 4
    assert transcript.metadata["survey_id"] == "survey_1"
