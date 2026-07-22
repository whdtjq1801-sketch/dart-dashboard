"""Scheduled batch job for the "히트맵" (KOSPI market-cap heatmap) tab.

Pulls the top KOSPI_HEATMAP_TOP_N stocks by market cap from FinanceDataReader
(no API key needed), classifies each not-yet-seen ticker into a sector via
one GPT call (a stock's sector essentially never changes, so this only costs
something the first time a ticker enters the top N), and upserts the daily
snapshot (market cap, % change, close price) into Postgres. The Flask app's
/api/kospi-heatmap endpoint only ever reads this table.

Meant to run on a schedule (Windows Task Scheduler locally, same pattern as
air_land_daily/daily_scan.py and market_issues_job.py) after KOSPI's close
(15:30 KST), e.g.:

    python kospi_heatmap_job.py

Requires the same .env as app.py (DATABASE_URL, OPENAI_API_KEY). Since the
Flask app is deployed on Railway, DATABASE_URL must point at that same
Postgres instance for this job's output to show up in the live dashboard.
"""
import sys
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from app import fetch_kospi_top_stocks, classify_sectors, get_existing_sectors, \
    save_kospi_snapshot, fetch_kospi_index, save_kospi_index_snapshot, \
    fetch_kospi_fundamentals, fetch_kospi_investor_flows, fetch_kospi_foreign_flows, \
    KOSPI_HEATMAP_TOP_N


def run():
    try:
        stocks = fetch_kospi_top_stocks(KOSPI_HEATMAP_TOP_N)
    except Exception as e:
        # A FinanceDataReader hiccup shouldn't leave yesterday's snapshot
        # silently stale with no error - fail loudly instead of no-op.
        print(f'could not fetch KOSPI listing: {e}', flush=True)
        return
    print(f'{len(stocks)} KOSPI stocks fetched (top {KOSPI_HEATMAP_TOP_N} by market cap)', flush=True)

    existing = get_existing_sectors([s['ticker'] for s in stocks])
    unclassified = [s for s in stocks if s['ticker'] not in existing]

    sectors = dict(existing)
    if unclassified:
        print(f'{len(unclassified)} new ticker(s), classifying sector via GPT', flush=True)
        try:
            sectors.update(classify_sectors(unclassified))
        except Exception as e:
            print(f'sector classification failed: {e}', flush=True)

    for s in stocks:
        # None (not a default like 'Industrials') for a ticker GPT never
        # returned - a fake-but-cached sector would otherwise look
        # "classified" and never get retried on a later run.
        s['sector'] = sectors.get(s['ticker'])

    try:
        fundamentals = fetch_kospi_fundamentals([s['ticker'] for s in stocks])
        print(f'fundamentals fetched for {len(fundamentals)}/{len(stocks)} tickers', flush=True)
    except Exception as e:
        print(f'could not fetch KRX fundamentals (PER/PBR/DIV): {e}', flush=True)
        fundamentals = {}
    for s in stocks:
        f = fundamentals.get(s['ticker'], {})
        s['per'] = f.get('per')
        s['pbr'] = f.get('pbr')
        s['div_yield'] = f.get('div_yield')

    try:
        foreign_flows = fetch_kospi_foreign_flows([s['ticker'] for s in stocks])
        print(f'foreign flows fetched for {len(foreign_flows)}/{len(stocks)} tickers', flush=True)
    except Exception as e:
        print(f'could not fetch KRX foreign flows (net buy/ownership): {e}', flush=True)
        foreign_flows = {}
    for s in stocks:
        ff = foreign_flows.get(s['ticker'], {})
        s['foreign_net_buy'] = ff.get('foreign_net_buy')
        s['foreign_ownership_pct'] = ff.get('foreign_ownership_pct')

    save_kospi_snapshot(stocks)

    try:
        idx = fetch_kospi_index()
        try:
            idx.update(fetch_kospi_investor_flows())
        except Exception as e:
            print(f'could not fetch KRX investor flows: {e}', flush=True)
        save_kospi_index_snapshot(idx)
        print(f"KOSPI {idx['kospi_close']:.2f} ({idx['kospi_change_pct']:+.2f}%), USD/KRW {idx['usd_krw']:.2f}", flush=True)
    except Exception as e:
        print(f'could not fetch/save KOSPI index snapshot: {e}', flush=True)

    print('kospi heatmap job done', flush=True)


if __name__ == '__main__':
    run()
