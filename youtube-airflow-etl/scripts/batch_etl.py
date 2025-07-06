import pandas as pd
from extract_youtube_data import batch_extract_youtube_channel_stats
from transform_kpis import transform_channel_batch, generate_channel_kpi_metrics
from load_to_postgres import load_to_postgres_batch
import os
from dotenv import load_dotenv

load_dotenv()

BATCH_SIZE = int(os.getenv("BATCH_SIZE"))  # YouTube API allows up to 50 IDs per batch
MAX_CHANNELS = int(os.getenv("MAX_CHANNELS"))  # You can increase this for high-volume channel analytics

def batch_run():
    df = pd.read_csv("data/unique_channels.csv")
    channel_ids = list(df["channel_id"].dropna().unique())[:MAX_CHANNELS]

    for i in range(0, len(channel_ids), BATCH_SIZE):
        batch_channel_ids = channel_ids[i:i+BATCH_SIZE]
        print(f"Processing channels {i+1} to {i+len(batch_channel_ids)}...")

        # Batch extract channel stats
        channel_stats = batch_extract_youtube_channel_stats(batch_channel_ids)

        # Transform
        transformed_channels = transform_channel_batch(channel_stats)
        kpi_metrics = generate_channel_kpi_metrics(channel_stats)

        # Load
        load_to_postgres_batch(transformed_channels, kpi_metrics)
        print(f"Loaded {len(transformed_channels)} channels and {len(kpi_metrics)} KPI rows.")