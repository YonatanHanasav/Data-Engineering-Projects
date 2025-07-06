import os
import requests
from dotenv import load_dotenv
from typing import List
import re

load_dotenv()

# Static mapping for YouTube video category IDs to names (partial, can be extended)
YOUTUBE_CATEGORY_MAP = {
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "18": "Short Movies",
    "19": "Travel & Events",
    "20": "Gaming",
    "21": "Videoblogging",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "29": "Nonprofits & Activism",
}

def parse_iso8601_duration_to_minutes(duration_str):
    # Example: PT1H2M3S, PT5M, PT1H
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration_str)
    if not match:
        return None
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    total_minutes = hours * 60 + minutes + (1 if seconds >= 30 else 0)  # round up if 30+ seconds
    return total_minutes

def extract_youtube_channel_stats(channel_id):
    api_key = os.getenv("YOUTUBE_API_KEY")

    url = (
        "https://www.googleapis.com/youtube/v3/channels"
        f"?part=statistics,snippet&id={channel_id}&key={api_key}"
    )
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    if not data["items"]:
        return None
    item = data["items"][0]
    stats = item["statistics"]
    snippet = item["snippet"]
    return {
        "channel_id": channel_id,
        "channel_name": snippet.get("title"),
        "channel_creation_date": snippet.get("publishedAt"),
        "view_count": int(stats.get("viewCount", 0)),
        "subscriber_count": int(stats.get("subscriberCount", 0)),
        "video_count": int(stats.get("videoCount", 0)),
    }


def extract_latest_video_metadata(channel_id):
    api_key = os.getenv("YOUTUBE_API_KEY")

    search_url = (
        "https://www.googleapis.com/youtube/v3/search"
        f"?key={api_key}&channelId={channel_id}&part=snippet&order=date&maxResults=1&type=video"
    )
    search_response = requests.get(search_url)
    search_response.raise_for_status()
    search_data = search_response.json()

    if not search_data["items"]:
        return None

    video_id = search_data["items"][0]["id"]["videoId"]
    title = search_data["items"][0]["snippet"]["title"]

    stats_url = (
        "https://www.googleapis.com/youtube/v3/videos"
        f"?key={api_key}&id={video_id}&part=statistics"
    )
    stats_response = requests.get(stats_url)
    stats_response.raise_for_status()
    stats = stats_response.json()["items"][0]["statistics"]

    return {
        "video_id": video_id,
        "title": title,
        "views": int(stats.get("viewCount", 0)),
        "likes": int(stats.get("likeCount", 0)),
        "comments": int(stats.get("commentCount", 0)),
        "engagement_rate": (
            (int(stats.get("likeCount", 0)) + int(stats.get("commentCount", 0))) /
            max(int(stats.get("viewCount", 1)), 1)
        )
    }


def batch_extract_youtube_channel_stats(channel_ids: List[str]):
    api_key = os.getenv("YOUTUBE_API_KEY")
    ids_str = ",".join(channel_ids)
    url = (
        "https://www.googleapis.com/youtube/v3/channels"
        f"?part=statistics,snippet,topicDetails&id={ids_str}&key={api_key}"
    )
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    results = []
    for item in data.get("items", []):
        stats = item["statistics"]
        snippet = item["snippet"]
        # New fields
        description = snippet.get("description")
        country = snippet.get("country")
        default_language = snippet.get("defaultLanguage")
        topics = None
        if "topicDetails" in item and "topicIds" in item["topicDetails"]:
            topics = ",".join(item["topicDetails"]["topicIds"])
        results.append({
            "channel_id": item["id"],
            "channel_name": snippet.get("title"),
            "channel_creation_date": snippet.get("publishedAt"),
            "view_count": int(stats.get("viewCount", 0)),
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "video_count": int(stats.get("videoCount", 0)),
            "description": description,
            "country": country,
            "topics": topics,
            "default_language": default_language,
        })
    return results


def batch_extract_youtube_video_stats(video_ids: List[str]):
    api_key = os.getenv("YOUTUBE_API_KEY")
    ids_str = ",".join(video_ids)
    url = (
        "https://www.googleapis.com/youtube/v3/videos"
        f"?part=snippet,statistics,contentDetails&id={ids_str}&key={api_key}"
    )
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    results = []
    for item in data.get("items", []):
        stats = item["statistics"]
        snippet = item["snippet"]
        content = item["contentDetails"]
        duration_minutes = parse_iso8601_duration_to_minutes(content.get("duration", ""))
        category_id = snippet.get("categoryId")
        category = YOUTUBE_CATEGORY_MAP.get(category_id, category_id)
        results.append({
            "video_id": item["id"],
            "title": snippet.get("title"),
            "publish_date": snippet.get("publishedAt"),
            "duration_minutes": duration_minutes,
            "category": category,
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
        })
    return results


def extract_latest_video_id(channel_id):
    api_key = os.getenv("YOUTUBE_API_KEY")
    search_url = (
        "https://www.googleapis.com/youtube/v3/search"
        f"?key={api_key}&channelId={channel_id}&part=snippet&order=date&maxResults=1&type=video"
    )
    search_response = requests.get(search_url)
    search_response.raise_for_status()
    search_data = search_response.json()
    if not search_data["items"]:
        return None
    return search_data["items"][0]["id"]["videoId"]