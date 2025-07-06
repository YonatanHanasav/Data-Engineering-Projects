from unittest.mock import patch, MagicMock
from scripts.load_to_postgres import load_to_postgres_batch
import pytest

@patch("scripts.load_to_postgres.psycopg2.connect")
def test_load_to_postgres_batch(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Provide minimal valid input
    channel_rows = [{
        "run_date": "2024-01-01",
        "channel_id": "abc",
        "channel_name": "Test",
        "channel_creation_date": "2020-01-01",
        "view_count": 1000,
        "subscriber_count": 100,
        "video_count": 10,
        "description": "desc",
        "country": "US"
    }]
    kpi_rows = [{
        "run_date": "2024-01-01",
        "channel_id": "abc",
        "channel_name": "Test",
        "subscriber_growth_rate": 0.1,
        "view_growth_rate": 0.2,
        "churn_flag": False,
        "views_per_video": 100,
        "subs_per_video": 10,
        "new_video_count": 1,
        "channel_age_days": 100,
        "avg_daily_views": 10,
        "avg_daily_subs": 1,
        "engagement_score": 0.5,
        "potential_revenue": 5.0,
        "monetization_flag": True,
        "virality_score": 0.3
    }]
    load_to_postgres_batch(channel_rows, kpi_rows)
    assert mock_connect.called
    assert mock_conn.cursor.called
    assert mock_cursor.execute.called

@patch("scripts.load_to_postgres.psycopg2.connect")
def test_load_to_postgres_batch_db_failure(mock_connect):
    # Simulate DB connection error
    mock_connect.side_effect = Exception("DB connection failed")
    channel_rows = []
    kpi_rows = []
    with pytest.raises(Exception):
        load_to_postgres_batch(channel_rows, kpi_rows) 