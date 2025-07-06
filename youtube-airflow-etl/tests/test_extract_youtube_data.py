import pytest
from unittest.mock import patch
from scripts.extract_youtube_data import extract_youtube_channel_stats

@patch("scripts.extract_youtube_data.requests.get")
def test_extract_youtube_channel_stats(mock_get):
    # Mock API response
    mock_get.return_value.json.return_value = {
        "items": [{
            "statistics": {"viewCount": "100", "subscriberCount": "10", "videoCount": "5"},
            "snippet": {"title": "Test Channel", "publishedAt": "2020-01-01T00:00:00Z"}
        }]
    }
    mock_get.return_value.raise_for_status = lambda: None

    result = extract_youtube_channel_stats("dummy_id")
    assert result["channel_name"] == "Test Channel"
    assert result["view_count"] == 100
    assert result["subscriber_count"] == 10
    assert result["video_count"] == 5

@patch("scripts.extract_youtube_data.requests.get")
def test_extract_youtube_channel_stats_api_failure(mock_get):
    # Simulate HTTP error
    def raise_http_error():
        raise Exception("API failure")
    mock_get.return_value.raise_for_status = raise_http_error
    with pytest.raises(Exception):
        extract_youtube_channel_stats("dummy_id") 