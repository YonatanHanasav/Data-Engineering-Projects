# --- youtube_etl.py ---
from extract_youtube_data import extract_youtube_channel_stats, extract_latest_video_metadata
from transform_kpis import transform_youtube_stats
from load_to_postgres import load_to_postgres
from datetime import datetime

def run_etl(channel_id):
    try:
        channel_data = extract_youtube_channel_stats(channel_id)
        if not channel_data:
            print(f"No data found for channel: {channel_id}")
            return
        transformed_channel_row = transform_youtube_stats(channel_data, channel_id)
        channel_name = channel_data.get("channel_name", "Unknown Channel")
        channel_creation_date = channel_data.get("channel_creation_date")

        video_metadata = extract_latest_video_metadata(channel_id)
        if video_metadata:
            video_row = {
                **video_metadata,
                "run_date": datetime.now().date(),
                "channel_id": channel_id,
                "channel_name": channel_name,
                "channel_creation_date": channel_creation_date
            }
        else:
            video_row = {
                "run_date": datetime.now().date(),
                "channel_id": channel_id,
                "channel_name": channel_name,
                "channel_creation_date": channel_creation_date,
                "video_id": "N/A",
                "title": "N/A",
                "views": 0,
                "likes": 0,
                "comments": 0,
                "engagement_rate": 0.0
            }

        load_to_postgres(transformed_channel_row, video_row)
        print(f"Successfully processed channel: {channel_name} ({channel_id})")
        
    except Exception as e:
        print(f"Failed for {channel_id}: {e}")
        # Don't re-raise the exception to continue processing other channels