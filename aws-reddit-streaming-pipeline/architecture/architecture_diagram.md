# AWS Reddit Streaming Pipeline Architecture

## System Architecture Diagram

```mermaid
graph TB
    %% External Services
    RedditAPI[Reddit API] 
    
    %% AWS Services
    EventBridge[EventBridge<br/>Hourly Trigger]
    Lambda[Lambda Function<br/>Reddit Ingestion]
    S3[S3 Bucket<br/>Data Lake]
    GlueCrawler[Glue Crawler<br/>Schema Discovery]
    DataCatalog[Glue Data Catalog<br/>Metadata]
    Athena[Athena<br/>SQL Queries]
    QuickSight[QuickSight<br/>Dashboard]
    
    %% Data Flow
    EventBridge -->|Triggers every hour| Lambda
    Lambda -->|Fetches top 10 posts| RedditAPI
    Lambda -->|Stores JSON Lines| S3
    S3 -->|Daily crawl| GlueCrawler
    GlueCrawler -->|Updates schema| DataCatalog
    DataCatalog -->|Provides metadata| Athena
    S3 -->|Query data| Athena
    Athena -->|Visualize data| QuickSight
    
    %% Styling
    classDef aws fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff
    classDef external fill:#FF6B6B,stroke:#333,stroke-width:2px,color:#fff
    classDef data fill:#4ECDC4,stroke:#333,stroke-width:2px,color:#fff
    
    class EventBridge,Lambda,S3,GlueCrawler,DataCatalog,Athena,QuickSight aws
    class RedditAPI external
    class S3 data
```

## Data Flow Diagram

```mermaid
sequenceDiagram
    participant EB as EventBridge
    participant L as Lambda
    participant R as Reddit API
    participant S3 as S3 Bucket
    participant GC as Glue Crawler
    participant DC as Data Catalog
    participant A as Athena
    participant QS as QuickSight
    
    Note over EB: Every Hour
    EB->>L: Trigger Lambda
    L->>R: Fetch hot posts
    R-->>L: Return JSON data
    L->>S3: Store JSON Lines
    Note over S3: raw/YYYY-MM-DD/reddit_data_HH_MM_SS.json
    
    Note over GC: Daily (00:00 UTC)
    GC->>S3: Crawl new data
    GC->>DC: Update schema
    DC-->>A: Provide metadata
    
    Note over A: On-demand
    A->>S3: Query data
    S3-->>A: Return results
    A-->>QS: Provide data for visualization
```

## Component Details

### 1. EventBridge
- **Purpose**: Triggers Lambda function every hour
- **Schedule**: `rate(1 hour)`
- **Cost**: Free tier eligible

### 2. Lambda Function
- **Runtime**: Python 3.9+
- **Memory**: 128 MB (sufficient for API calls)
- **Timeout**: 30 seconds
- **Dependencies**: `boto3`, `praw`

### 3. S3 Bucket
- **Structure**: `s3://bucket/raw/YYYY-MM-DD/`
- **Format**: JSON Lines (one object per line)
- **Partitioning**: Date-based for efficient querying

### 4. Glue Crawler
- **Frequency**: Daily at 00:00 UTC
- **Output**: Updates Data Catalog table
- **Schema**: Auto-discovered from JSON structure

### 5. Athena
- **Engine**: Presto (serverless)
- **Storage**: Pay per query
- **Format**: JSON with Glue catalog

### 6. QuickSight
- **Data Source**: Athena
- **Refresh**: Hourly (manual or scheduled)
- **Visualizations**: Time series, bar charts, tables 