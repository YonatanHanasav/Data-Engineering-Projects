# AWS Glue Crawler Setup Notes

This document outlines the configuration for AWS Glue Crawler to automatically discover and catalog Reddit data stored in S3.

## Crawler Configuration

### 1. Create Database
- **Database Name**: `reddit_data_db`
- **Description**: Database for Reddit streaming pipeline data

### 2. Create Crawler
- **Crawler Name**: `reddit_data_crawler`
- **Description**: Crawls Reddit JSON data from S3

### 3. Data Store Configuration
- **Data Store**: S3
- **Include Path**: `s3://your-bucket-name/raw/`
- **Exclude Path**: Leave empty (no exclusions needed)

### 4. IAM Role
- **Role**: Create or use existing role with Glue service permissions
- **Required Permissions**:
  - `s3:GetObject` on your S3 bucket
  - `glue:*` permissions for catalog operations

### 5. Schedule Configuration
- **Frequency**: Daily
- **Start Time**: 00:00 UTC (or your preferred time)
- **Timezone**: UTC

### 6. Output Configuration
- **Database**: `reddit_data_db`
- **Table Prefix**: `reddit_` (optional)
- **Table Name**: `posts` (or let it auto-generate)

## Schema Discovery Settings

### Classifier Configuration
- **Classifier**: None (JSON format is auto-detected)
- **JSON Path**: `$[*]` (for JSON Lines format)

### Schema Updates
- **Schema Change Behavior**: Update the table definition in the data catalog
- **Object Deletion**: Log a warning

## Table Structure

The crawler will create a table with the following columns:
- `title` (string)
- `author` (string)
- `score` (int)
- `num_comments` (int)
- `created_utc` (bigint)
- `subreddit` (string)
- `url` (string)
- `selftext` (string)
- `is_self` (boolean)
- `over_18` (boolean)
- `spoiler` (boolean)
- `stickied` (boolean)
- `subreddit_subscribers` (int)
- `upvote_ratio` (double)

## Testing the Crawler

1. **Manual Run**: Execute the crawler manually to test
2. **Check Logs**: Review CloudWatch logs for any errors
3. **Verify Table**: Check that the table is created in the Glue Data Catalog
4. **Test Query**: Run a simple Athena query to verify data access

## Troubleshooting

### Common Issues
- **Permission Errors**: Ensure IAM role has proper S3 and Glue permissions
- **Schema Issues**: Check JSON format consistency in S3
- **No Data Found**: Verify S3 path and data existence

### Monitoring
- **CloudWatch Logs**: Monitor crawler execution logs
- **Metrics**: Track crawler success/failure rates
- **Alerts**: Set up CloudWatch alarms for crawler failures 