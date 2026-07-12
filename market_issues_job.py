"""Scheduled batch job for the "시황" (market issues) tab.

Fetches each configured YouTube channel's latest uploads (RSS, no API key
needed), skips videos already in the video_summaries table, and for every
new video: pulls the Korean transcript, summarizes it with GPT, and stores
the result. The Flask app's /api/market-issues endpoint only ever reads
from this table - it never calls YouTube or OpenAI itself - so this job is
what keeps the tab's content fresh.

Meant to run on a schedule (Windows Task Scheduler locally, same pattern as
air_land_daily/daily_scan.py), e.g. once or twice a day:

    python market_issues_job.py

Requires the same .env as app.py (DATABASE_URL, OPENAI_API_KEY). Since the
Flask app is deployed on Railway, DATABASE_URL must point at that same
Postgres instance for this job's output to show up in the live dashboard.
"""
import os
from app import MARKET_CHANNELS, fetch_channel_videos, fetch_video_transcript, \
    summarize_video_with_gpt, get_db, save_video_summary, MAX_VIDEOS_PER_CHANNEL


def video_exists(video_id):
    sql = "SELECT 1 FROM video_summaries WHERE video_id = %s"
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (video_id,))
            return cur.fetchone() is not None


def run():
    for channel_name, channel_id in MARKET_CHANNELS.items():
        videos = fetch_channel_videos(channel_id, max_results=MAX_VIDEOS_PER_CHANNEL)
        print(f'[{channel_name}] {len(videos)} videos in feed', flush=True)
        for v in videos:
            if video_exists(v['video_id']):
                continue
            print(f"[{channel_name}] new video, summarizing: {v['title']}", flush=True)
            try:
                transcript = fetch_video_transcript(v['video_id'])
                summary = summarize_video_with_gpt(channel_name, v['title'], transcript)
                save_video_summary(
                    v['video_id'], channel_name, v['title'], v['published_at'], summary
                )
            except Exception as e:
                # Leave it out of the table - next run will retry it.
                print(f"[{channel_name}] failed on {v['video_id']}: {e}", flush=True)
    print('market issues job done', flush=True)


if __name__ == '__main__':
    run()
