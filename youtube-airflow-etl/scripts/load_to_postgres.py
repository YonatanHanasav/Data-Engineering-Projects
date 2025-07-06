import psycopg2
from typing import List, Dict
import os
from dotenv import load_dotenv
load_dotenv()

def load_to_postgres_batch(channel_rows: List[Dict], kpi_rows: List[Dict]):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cur = conn.cursor()

        # Create schema if needed
        cur.execute("CREATE SCHEMA IF NOT EXISTS youtube_kpi;")
        conn.commit()

        # Create channels table (no topics, no default_language)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS youtube_kpi.channels (
                run_date DATE,
                channel_id TEXT,
                channel_name TEXT,
                channel_creation_date DATE,
                view_count BIGINT,
                subscriber_count BIGINT,
                video_count BIGINT,
                description TEXT,
                country TEXT,
                PRIMARY KEY (run_date, channel_id)
            );
        """)
        conn.commit()

        # Create channel_kpis table with new KPIs and channel_name
        cur.execute("""
            CREATE TABLE IF NOT EXISTS youtube_kpi.channel_kpis (
                run_date DATE,
                channel_id TEXT,
                channel_name TEXT,
                subscriber_growth_rate FLOAT,
                view_growth_rate FLOAT,
                churn_flag BOOLEAN,
                views_per_video FLOAT,
                subs_per_video FLOAT,
                new_video_count BIGINT,
                channel_age_days INT,
                avg_daily_views FLOAT,
                avg_daily_subs FLOAT,
                engagement_score FLOAT,
                potential_revenue FLOAT,
                monetization_flag BOOLEAN,
                virality_score FLOAT,
                PRIMARY KEY (run_date, channel_id)
            );
        """)
        conn.commit()

        # Insert channel rows
        for row in channel_rows:
            cur.execute("""
                INSERT INTO youtube_kpi.channels (
                    run_date, channel_id, channel_name, channel_creation_date,
                    view_count, subscriber_count, video_count,
                    description, country
                )
                VALUES (%(run_date)s, %(channel_id)s, %(channel_name)s, %(channel_creation_date)s,
                        %(view_count)s, %(subscriber_count)s, %(video_count)s,
                        %(description)s, %(country)s)
                ON CONFLICT (run_date, channel_id) DO UPDATE
                SET channel_name = EXCLUDED.channel_name,
                    channel_creation_date = EXCLUDED.channel_creation_date,
                    view_count = EXCLUDED.view_count,
                    subscriber_count = EXCLUDED.subscriber_count,
                    video_count = EXCLUDED.video_count,
                    description = EXCLUDED.description,
                    country = EXCLUDED.country;
            """, row)

        # Insert KPI rows
        for row in kpi_rows:
            cur.execute("""
                INSERT INTO youtube_kpi.channel_kpis (
                    run_date, channel_id, channel_name, subscriber_growth_rate, view_growth_rate, churn_flag,
                    views_per_video, subs_per_video, new_video_count, channel_age_days,
                    avg_daily_views, avg_daily_subs, engagement_score, potential_revenue, monetization_flag, virality_score
                )
                VALUES (%(run_date)s, %(channel_id)s, %(channel_name)s, %(subscriber_growth_rate)s, %(view_growth_rate)s, %(churn_flag)s,
                        %(views_per_video)s, %(subs_per_video)s, %(new_video_count)s, %(channel_age_days)s,
                        %(avg_daily_views)s, %(avg_daily_subs)s, %(engagement_score)s, %(potential_revenue)s, %(monetization_flag)s, %(virality_score)s)
                ON CONFLICT (run_date, channel_id) DO UPDATE
                SET channel_name = EXCLUDED.channel_name,
                    subscriber_growth_rate = EXCLUDED.subscriber_growth_rate,
                    view_growth_rate = EXCLUDED.view_growth_rate,
                    churn_flag = EXCLUDED.churn_flag,
                    views_per_video = EXCLUDED.views_per_video,
                    subs_per_video = EXCLUDED.subs_per_video,
                    new_video_count = EXCLUDED.new_video_count,
                    channel_age_days = EXCLUDED.channel_age_days,
                    avg_daily_views = EXCLUDED.avg_daily_views,
                    avg_daily_subs = EXCLUDED.avg_daily_subs,
                    engagement_score = EXCLUDED.engagement_score,
                    potential_revenue = EXCLUDED.potential_revenue,
                    monetization_flag = EXCLUDED.monetization_flag,
                    virality_score = EXCLUDED.virality_score;
            """, row)

        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[load_to_postgres_batch] Error: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()