-- Example Athena SQL queries for Reddit data analysis
-- Database: reddit_streaming_db, Table: raw

-- Query 1: Get top posts by score for a specific date
SELECT 
    title,
    author,
    score,
    num_comments,
    created_utc,
    subreddit
FROM reddit_streaming_db.raw
WHERE SUBSTRING(created_utc, 1, 10) = '2025-07-05'
ORDER BY score DESC
LIMIT 10;

-- Query 2: Get posts by subreddit with average scores
SELECT 
    subreddit,
    COUNT(*) as post_count,
    AVG(score) as avg_score,
    AVG(num_comments) as avg_comments
FROM reddit_streaming_db.raw
GROUP BY subreddit
ORDER BY avg_score DESC;

-- Query 3: Get hourly post trends
SELECT 
    SUBSTRING(created_utc, 1, 10) as post_date,
    SUBSTRING(created_utc, 12, 2) as post_hour,
    COUNT(*) as posts_count,
    AVG(score) as avg_score
FROM reddit_streaming_db.raw
GROUP BY SUBSTRING(created_utc, 1, 10), SUBSTRING(created_utc, 12, 2)
ORDER BY post_date DESC, post_hour;

-- Query 4: Get top authors by total score
SELECT 
    author,
    COUNT(*) as posts_count,
    SUM(score) as total_score,
    AVG(score) as avg_score
FROM reddit_streaming_db.raw
WHERE author != '[deleted]'
GROUP BY author
ORDER BY total_score DESC
LIMIT 20;

-- Query 5: Get recent posts with high engagement
SELECT 
    title,
    author,
    subreddit,
    score,
    num_comments,
    created_utc,
    fetched_utc
FROM reddit_streaming_db.raw
WHERE score > 1000
ORDER BY created_utc DESC
LIMIT 20;

-- Query 6: Get posts by content type (image, video, text)
SELECT 
    post_hint,
    COUNT(*) as post_count,
    AVG(score) as avg_score,
    AVG(num_comments) as avg_comments
FROM reddit_streaming_db.raw
WHERE post_hint IS NOT NULL
GROUP BY post_hint
ORDER BY avg_score DESC; 