import json
import boto3
import datetime
import os
import praw
import re

# Utility to remove emojis
def remove_emojis(text):
    return re.sub(r'[\U00010000-\U0010ffff]', '', text)

def lambda_handler(event, context):
    try:
        # Setup Reddit client
        reddit = praw.Reddit(
            client_id=os.environ['REDDIT_CLIENT_ID'],
            client_secret=os.environ['REDDIT_CLIENT_SECRET'],
            user_agent="StreamBotApp/0.1 by YonatanHPrivate",
            username=os.environ['REDDIT_USERNAME'],
            password=os.environ['REDDIT_PASSWORD']
        )

        subreddit_name = os.environ.get("SUBREDDIT", "all")
        subreddit = reddit.subreddit(subreddit_name)

        now = datetime.datetime.utcnow()
        iso_fetched_time = now.isoformat()

        posts = []
        for post in subreddit.hot(limit=10):
            posts.append({
                "id": post.id,
                "title": remove_emojis(post.title),
                "author": str(post.author),
                "subreddit": post.subreddit.display_name,
                "score": post.score,
                "num_comments": post.num_comments,
                "url": post.url,
                "permalink": post.permalink,
                "selftext": remove_emojis(post.selftext or ""),
                "created_utc": datetime.datetime.utcfromtimestamp(post.created_utc).isoformat(),
                "fetched_utc": iso_fetched_time,
                "over_18": post.over_18,
                "is_video": post.is_video,
                "post_hint": getattr(post, "post_hint", None),
                "thumbnail": post.thumbnail,
                "link_flair_text": post.link_flair_text,
                "domain": post.domain
            })

        # Save file in raw/YYYY-MM-DD/reddit_data_HH_MM_SS.json
        file_name = f"reddit_data_{now.strftime('%H_%M_%S')}.json"
        key = f"raw/{now.strftime('%Y-%m-%d')}/{file_name}"

        # Upload as JSON Lines (one object per line — required for Glue to work without a classifier)
        json_lines = "\n".join(json.dumps(post) for post in posts)

        s3 = boto3.client('s3')
        bucket_name = os.environ['S3_BUCKET']
        s3.put_object(Bucket=bucket_name, Key=key, Body=json_lines)

        print(f"Stored {len(posts)} posts to {key}")

        return {
            "status": "success",
            "records": len(posts)
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            "status": "error",
            "message": str(e)
        }