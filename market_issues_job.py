"""Scheduled batch job for the "시황" (market issues) tab.

Fetches each configured YouTube channel's latest uploads (RSS, no API key
needed), skips videos already in the video_summaries table, and for every
new video: pulls the Korean transcript, summarizes it with GPT (English
title + English summary + Korean summary + market sentiment label + topic
tags, in one call), and stores the result. Also backfills
title_en/summary_en/sentiment/topics for any older row missing them (from
before those fields existed), by working from the stored content instead
of re-fetching the transcript.

The Flask app's /api/market-issues endpoint only ever reads from this
table - it never calls YouTube or OpenAI itself - so this job is what
keeps the tab's content fresh.

Meant to run on a schedule (Windows Task Scheduler locally, same pattern as
air_land_daily/daily_scan.py), e.g. once or twice a day:

    python market_issues_job.py

Requires the same .env as app.py (DATABASE_URL, OPENAI_API_KEY). Since the
Flask app is deployed on Railway, DATABASE_URL must point at that same
Postgres instance for this job's output to show up in the live dashboard.
"""
import sys
# Windows Task Scheduler runs this with the console's legacy codepage
# (cp949 for Korean Windows), which can't encode every character YouTube
# titles throw at it (fancy separators, emoji, etc.) - that crashes the
# whole run on a single print(). Force UTF-8 with lossy fallback instead.
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from app import MARKET_CHANNELS, fetch_channel_videos, fetch_video_transcript, \
    summarize_video_bilingual, translate_summary_to_english, get_db, \
    save_video_summary, get_rows_missing_english, update_video_summary_en, \
    MAX_VIDEOS_PER_CHANNEL


def video_exists(video_id):
    sql = "SELECT 1 FROM video_summaries WHERE video_id = %s"
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (video_id,))
            return cur.fetchone() is not None


def backfill_missing_english():
    rows = get_rows_missing_english()
    if not rows:
        return
    print(f'{len(rows)} row(s) missing English fields, backfilling', flush=True)
    for r in rows:
        try:
            translated = translate_summary_to_english(r['channel_name'], r['title'], r['summary'])
            update_video_summary_en(
                r['video_id'], translated['title_en'], translated['summary_en'],
                translated['sentiment'], translated['topics']
            )
        except Exception as e:
            print(f"backfill failed for {r['video_id']}: {e}", flush=True)


def run():
    backfill_missing_english()

    for channel_name, channel_id in MARKET_CHANNELS.items():
        try:
            videos = fetch_channel_videos(channel_id, max_results=MAX_VIDEOS_PER_CHANNEL)
        except Exception as e:
            # One channel's RSS feed being unreachable shouldn't stop the
            # other channels from being processed.
            print(f'[{channel_name}] could not fetch feed: {e}', flush=True)
            continue
        print(f'[{channel_name}] {len(videos)} videos in feed', flush=True)
        for v in videos:
            if video_exists(v['video_id']):
                continue
            print(f"[{channel_name}] new video, summarizing: {v['title']}", flush=True)
            try:
                transcript = fetch_video_transcript(v['video_id'])
                result = summarize_video_bilingual(channel_name, v['title'], transcript)
                save_video_summary(
                    v['video_id'], channel_name, v['title'], result['title_en'],
                    v['published_at'], result['summary_ko'], result['summary_en'],
                    result['sentiment'], result['topics']
                )
            except Exception as e:
                # Leave it out of the table - next run will retry it.
                print(f"[{channel_name}] failed on {v['video_id']}: {e}", flush=True)
    print('market issues job done', flush=True)


if __name__ == '__main__':
    run()
