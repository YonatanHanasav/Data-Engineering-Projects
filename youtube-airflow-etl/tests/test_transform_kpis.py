from scripts.transform_kpis import generate_channel_kpi_metrics
from unittest.mock import patch, MagicMock

@patch("scripts.transform_kpis.psycopg2.connect")
def test_generate_channel_kpi_metrics(mock_connect):
    # Mock DB connection for get_previous_channel_metrics
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None  # Simulate no previous metrics

    # Provide mock input
    channel_stats = [{
        "channel_id": "abc",
        "channel_name": "Test",
        "channel_creation_date": "2020-01-01T00:00:00Z",
        "view_count": 1000,
        "subscriber_count": 100,
        "video_count": 10,
        "description": "desc",
        "country": "US"
    }]
    kpis = generate_channel_kpi_metrics(channel_stats)
    assert isinstance(kpis, list)
    assert kpis[0]["views_per_video"] == 100 