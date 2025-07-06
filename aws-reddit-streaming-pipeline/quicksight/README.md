# QuickSight Dashboard Setup

This folder contains QuickSight dashboard configurations and screenshots for the Reddit streaming pipeline.

## Dashboard Overview

The QuickSight dashboard visualizes Reddit data trends including:
- Hourly post score changes
- Top posts by subreddit
- Author performance metrics
- Temporal trends and patterns

## Setup Instructions

1. **Connect to Athena Data Source**
   - In QuickSight, create a new data source
   - Select "Athena" as the data source type
   - Connect to your AWS account
   - Select the database and table created by AWS Glue Crawler

2. **Create Visualizations**
   - **Time Series Chart**: Post scores over time
   - **Bar Chart**: Top posts by score
   - **Pie Chart**: Posts by subreddit distribution
   - **Table**: Top authors with metrics

3. **Dashboard Layout**
   - Arrange visualizations in a logical flow
   - Add filters for date range and subreddit
   - Include title and description

## Sample Dashboard Screenshots

Add your dashboard screenshots here:
- `dashboard_overview.png` - Main dashboard view
- `trends_analysis.png` - Time series analysis
- `subreddit_breakdown.png` - Subreddit performance

## Refresh Schedule

Configure the dashboard to refresh every hour to match the Lambda ingestion schedule.

## Tips

- Use calculated fields for derived metrics
- Apply filters to focus on specific time periods
- Use conditional formatting for score thresholds
- Export data for further analysis if needed 