from datetime import datetime
import psycopg2
from typing import List, Dict


def get_previous_channel_metrics(channel_id):
    conn = psycopg2.connect(
        dbname="youtube_db",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT view_count, subscriber_count, video_count
            FROM youtube_kpi.channels
            WHERE channel_id = %s
            ORDER BY run_date DESC
            LIMIT 1;
        """, (channel_id,))

        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            return {
                "view_count": row[0],
                "subscriber_count": row[1],
                "video_count": row[2]
            }
    except psycopg2.errors.UndefinedTable:
        # Table doesn't exist yet, return default values
        cur.close()
        conn.close()
        pass
    except Exception as e:
        # Handle any other database errors
        cur.close()
        conn.close()
        print(f"Database error in get_previous_channel_metrics: {e}")
        pass
    
    return {
        "view_count": 0,
        "subscriber_count": 0,
        "video_count": 0
    }


def transform_channel_batch(channel_data_list: List[Dict], run_date=None):
    if run_date is None:
        run_date = datetime.now().date()
    transformed = []
    for channel_data in channel_data_list:
        transformed.append({
            "run_date": run_date,
            "channel_id": channel_data["channel_id"],
            "channel_name": channel_data.get("channel_name"),
            "channel_creation_date": channel_data.get("channel_creation_date"),
            "view_count": channel_data.get("view_count", 0),
            "subscriber_count": channel_data.get("subscriber_count", 0),
            "video_count": channel_data.get("video_count", 0),
            "description": channel_data.get("description"),
            "country": channel_data.get("country"),
            "topics": channel_data.get("topics"),
            "default_language": channel_data.get("default_language"),
        })
    return transformed


def transform_video_batch(video_data_list: List[Dict], run_date=None):
    if run_date is None:
        run_date = datetime.now().date()
    transformed = []
    for video_data in video_data_list:
        transformed.append({
            "run_date": run_date,
            "video_id": video_data.get("video_id"),
            "channel_id": video_data.get("channel_id"),
            "title": video_data.get("title"),
            "publish_date": video_data.get("publish_date"),
            "duration_minutes": video_data.get("duration_minutes"),
            "category": video_data.get("category"),
            "views": video_data.get("views", 0),
            "likes": video_data.get("likes", 0),
            "comments": video_data.get("comments", 0),
        })
    return transformed


def generate_channel_kpi_metrics(channel_data_list: List[Dict], run_date=None):
    if run_date is None:
        run_date = datetime.now().date()
    kpi_metrics = []
    for channel_data in channel_data_list:
        channel_id = channel_data["channel_id"]
        channel_name = channel_data.get("channel_name")
        previous = get_previous_channel_metrics(channel_id)
        current_views = channel_data.get("view_count", 0)
        prev_views = previous["view_count"]
        current_subs = channel_data.get("subscriber_count", 0)
        prev_subs = previous["subscriber_count"]
        current_videos = channel_data.get("video_count", 0)
        prev_videos = previous["video_count"]
        # Growth rates
        if prev_subs > 0:
            subscriber_growth_rate = round((current_subs - prev_subs) / prev_subs, 4)
        else:
            subscriber_growth_rate = None
        if prev_views > 0:
            view_growth_rate = round((current_views - prev_views) / prev_views, 4)
        else:
            view_growth_rate = None
        churn_flag = subscriber_growth_rate is not None and subscriber_growth_rate < 0
        # Per video metrics
        views_per_video = round(current_views / max(current_videos, 1), 2)
        subs_per_video = round(current_subs / max(current_videos, 1), 2)
        new_video_count = current_videos - prev_videos
        # Channel age
        creation_date = channel_data.get("channel_creation_date")
        if creation_date:
            try:
                creation_dt = datetime.fromisoformat(creation_date.replace('Z', '+00:00'))
                channel_age_days = (run_date - creation_dt.date()).days
            except Exception:
                channel_age_days = None
        else:
            channel_age_days = None
        # KPIs
        avg_daily_views = round(current_views / max(channel_age_days, 1), 2) if channel_age_days else None
        avg_daily_subs = round(current_subs / max(channel_age_days, 1), 2) if channel_age_days else None
        engagement_score = round((current_views * current_subs) / 1e6, 2) if current_views and current_subs else None
        # Potential revenue: CPM $2 per 1000 views (example)
        potential_revenue = round((current_views / 1000) * 2, 2) if current_views else None
        monetization_flag = (current_subs >= 1000 and current_views >= 4000) if current_subs is not None and current_views is not None else None
        # Virality score: recent view growth vs. average (simple placeholder)
        virality_score = None
        if avg_daily_views and prev_views > 0:
            recent_growth = current_views - prev_views
            virality_score = round(recent_growth / max(avg_daily_views, 1), 2)
        kpi_metrics.append({
            "run_date": run_date,
            "channel_id": channel_id,
            "channel_name": channel_name,
            "subscriber_growth_rate": subscriber_growth_rate,
            "view_growth_rate": view_growth_rate,
            "churn_flag": churn_flag,
            "views_per_video": views_per_video,
            "subs_per_video": subs_per_video,
            "new_video_count": new_video_count,
            "channel_age_days": channel_age_days,
            "avg_daily_views": avg_daily_views,
            "avg_daily_subs": avg_daily_subs,
            "engagement_score": engagement_score,
            "potential_revenue": potential_revenue,
            "monetization_flag": monetization_flag,
            "virality_score": virality_score,
        })
    return kpi_metrics


def transform_channel_stats_batch(channel_data_list: List[Dict], run_date=None):
    if run_date is None:
        run_date = datetime.now().date()
    transformed = []
    for channel_data in channel_data_list:
        channel_id = channel_data["channel_id"]
        previous = get_previous_channel_metrics(channel_id)
        current_subs = channel_data.get("subscriber_count", 0)
        prev_subs = previous["subscriber_count"]
        # Growth rate: (today - yesterday) / yesterday (avoid div by zero)
        if prev_subs > 0:
            growth_rate = round((current_subs - prev_subs) / prev_subs, 4)
        else:
            growth_rate = None
        churn_flag = growth_rate is not None and growth_rate < 0
        transformed.append({
            "run_date": run_date,
            "channel_id": channel_id,
            "channel_name": channel_data.get("channel_name"),
            "channel_creation_date": channel_data.get("channel_creation_date"),
            "view_count": channel_data.get("view_count", 0),
            "subscriber_count": current_subs,
            "video_count": channel_data.get("video_count", 0),
            "subscriber_growth_rate": growth_rate,
            "churn_flag": churn_flag,
            # You can add daily deltas and other fields as needed
        })
    return transformed


def transform_video_stats_batch(video_data_list: List[Dict], run_date=None):
    if run_date is None:
        run_date = datetime.now().date()
    transformed = []
    for video_data in video_data_list:
        transformed.append({
            "run_date": run_date,
            "video_id": video_data.get("video_id"),
            "channel_id": video_data.get("channel_id"),
            "title": video_data.get("title"),
            "publish_date": video_data.get("publish_date"),
            "duration": video_data.get("duration"),
            "category_id": video_data.get("category_id"),
            "views": video_data.get("views", 0),
            "likes": video_data.get("likes", 0),
            "dislikes": video_data.get("dislikes"),
            "comments": video_data.get("comments", 0),
            "like_dislike_ratio": video_data.get("like_dislike_ratio"),
        })
    return transformed


def transform_youtube_stats(channel_data, channel_id):
    today = datetime.now().date()
    # channel_data is the structured dict from extract_youtube_channel_stats
    channel_name = channel_data.get("channel_name", "Unknown Channel")
    channel_creation_date = channel_data.get("channel_creation_date")
    current = {
        "view_count": channel_data.get("view_count", 0),
        "subscriber_count": channel_data.get("subscriber_count", 0),
        "video_count": channel_data.get("video_count", 0)
    }
    previous = get_previous_channel_metrics(channel_id)

    return {
        "run_date": today,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_creation_date": channel_creation_date,
        "view_count": current["view_count"],
        "subscriber_count": current["subscriber_count"],
        "video_count": current["video_count"],
        "daily_view_delta": current["view_count"] - previous["view_count"],
        "daily_subscriber_delta": current["subscriber_count"] - previous["subscriber_count"],
        "daily_video_delta": current["video_count"] - previous["video_count"],
        "avg_views_per_video": round(
            current["view_count"] / max(current["video_count"], 1), 2
        ),
        "subscriber_per_video": round(
            current["subscriber_count"] / max(current["video_count"], 1), 2
        )
    }