# Architecture Documentation

This folder contains the architecture diagram and documentation for the AWS Reddit Streaming Pipeline.

## Architecture Overview

The pipeline follows a serverless architecture pattern using AWS services:

```
Reddit API → Lambda → S3 → Glue Crawler → Athena → QuickSight
```

## Components

1. **AWS Lambda** - Serverless function that fetches Reddit data hourly
2. **Amazon S3** - Data lake for raw JSON data storage
3. **AWS Glue** - Schema discovery and data catalog management
4. **Amazon Athena** - Serverless SQL query engine
5. **Amazon QuickSight** - Business intelligence and visualization

## Data Flow

1. **Ingestion**: Lambda function triggered by EventBridge (hourly)
2. **Storage**: Raw data stored in S3 with date-based partitioning
3. **Cataloging**: Glue Crawler discovers schema and updates Data Catalog
4. **Querying**: Athena provides SQL access to the data
5. **Visualization**: QuickSight creates interactive dashboards

## Architecture Diagram

Add your architecture diagram here:
- `architecture_diagram.png` - Main system architecture
- `data_flow_diagram.png` - Detailed data flow
- `deployment_diagram.png` - AWS resource deployment

```mermaid
graph LR
    A[Reddit API] --> B[Lambda]
    B --> C[S3]
    C --> D[Glue Crawler]
    D --> E[Data Catalog]
    E --> F[Athena]
    F --> G[QuickSight]
```
## Security Considerations

- **IAM**: Least privilege access for all services
- **S3**: Bucket policies and encryption
- **Monitoring**: CloudWatch logs and metrics 