import pandas as pd

df = pd.read_csv('data/US_youtube_trending_data.csv')

# Keep only channelId, channelTitle and view_count
channels_df = df[['channelId', 'channelTitle', 'view_count']]

# Group by channelId and channelTitle, sum view_count
agg_df = channels_df.groupby(['channelId', 'channelTitle'], as_index=False).sum()

# Rename channelId to channel_id for output
agg_df = agg_df.rename(columns={'channelId': 'channel_id'})

# Order by sum views descending
agg_df = agg_df.sort_values('view_count', ascending=False)

# Save to unique_channels.csv with the same column names
agg_df.to_csv('data/unique_channels.csv', index=False)
