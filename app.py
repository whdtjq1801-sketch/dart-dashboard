import os, json, time, re, zipfile, io, threading
from collections import defaultdict, deque
import psycopg2
from psycopg2.extras import RealDictCursor
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template
import requests as http
import yfinance as yf
from openai import OpenAI
from youtube_transcript_api import YouTubeTranscriptApi
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

DART_API_KEY       = os.getenv('DART_API_KEY')
OPENAI_API_KEY     = os.getenv('OPENAI_API_KEY')
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_TOKEN  = os.getenv("ADMIN_TOKEN")

CORP_CACHE_FILE     = os.path.join(BASE_DIR, 'corp_map_cache.json')
TICKER_CACHE_FILE   = os.path.join(BASE_DIR, 'ticker_corp_cache.json')
STOCK_CACHE_FILE    = os.path.join(BASE_DIR, 'stock_code_cache.json')
ENG_NAME_CACHE_FILE = os.path.join(BASE_DIR, 'eng_name_cache.json')
COMPANIES_DATASET_FILE = os.path.join(BASE_DIR, 'static', 'companies.json')
CACHE_MAX_DAYS   = 7

openai_client = OpenAI(api_key=OPENAI_API_KEY)

# ── searchable listed-companies dataset (prebuilt, shipped in the repo) ──
def load_companies_dataset():
    if not os.path.exists(COMPANIES_DATASET_FILE):
        return []
    with open(COMPANIES_DATASET_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

_companies_dataset = load_companies_dataset()
_dataset_by_kr     = {c['name_kr']: c for c in _companies_dataset}
_dataset_by_ticker = {c['ticker']: c for c in _companies_dataset}

# ── helpers ────────────────────────────────────────────
def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다.")
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor
    )

def get_user_key():
    return (
        request.headers.get("X-User-Key")
        or request.remote_addr
        or "unknown"
    )

EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')

def require_user_email():
    """Watchlist endpoints are per-user, identified by the email the frontend
    sends as X-User-Key. Returns the email, or None if missing/invalid
    (caller should respond 401)."""
    email = (request.headers.get("X-User-Key") or "").strip().lower()
    if not EMAIL_RE.match(email):
        return None
    return email

# ── rate limiting ──────────────────────────────────────
# In-memory sliding-window limiter. Good enough for a single gunicorn worker
# (see Procfile); the goal is just to stop a runaway script or bot from
# burning through the OpenAI/DART API quota, not perfect fairness.
_rate_limit_lock = threading.Lock()
_rate_limit_hits = defaultdict(deque)

def rate_limited(key, max_calls, window_sec):
    """Returns True if `key` has made >= max_calls calls in the last
    window_sec seconds (and records this call if not)."""
    now = time.time()
    with _rate_limit_lock:
        hits = _rate_limit_hits[(key, max_calls, window_sec)]
        while hits and hits[0] < now - window_sec:
            hits.popleft()
        if len(hits) >= max_calls:
            return True
        hits.append(now)
        return False

def rate_limit_key():
    return request.headers.get("X-User-Key") or request.remote_addr or "unknown"

def init_db():
    sql = """
    CREATE TABLE IF NOT EXISTS search_logs (
        id BIGSERIAL PRIMARY KEY,
        user_key VARCHAR(100),
        ip_address VARCHAR(50),
        user_agent TEXT,
        keyword VARCHAR(200),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS disclosure_results (
        id BIGSERIAL PRIMARY KEY,
        user_key VARCHAR(100),
        corp_name VARCHAR(100),
        stock_code VARCHAR(20),
        rcept_no VARCHAR(30),
        report_nm VARCHAR(300),
        rcept_dt VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (user_key, rcept_no)
    );

    CREATE TABLE IF NOT EXISTS disclosure_interpretations (
        id BIGSERIAL PRIMARY KEY,
        user_key VARCHAR(100),
        corp_name VARCHAR(100),
        stock_code VARCHAR(20),
        rcept_no VARCHAR(30),
        report_nm VARCHAR(300),
        summary TEXT,
        price NUMERIC(20, 4),
        change_rate NUMERIC(10, 4),
        volume BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS users (
        user_key VARCHAR(255) PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS watchlist (
        id BIGSERIAL PRIMARY KEY,
        user_key VARCHAR(255) NOT NULL,
        corp_name_kr VARCHAR(200) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (user_key, corp_name_kr)
    );

    CREATE TABLE IF NOT EXISTS video_summaries (
        id BIGSERIAL PRIMARY KEY,
        video_id VARCHAR(20) NOT NULL UNIQUE,
        channel_name VARCHAR(100),
        title VARCHAR(300),
        title_en VARCHAR(300),
        published_at VARCHAR(40),
        summary TEXT,
        summary_en TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    ALTER TABLE video_summaries ADD COLUMN IF NOT EXISTS title_en VARCHAR(300);
    ALTER TABLE video_summaries ADD COLUMN IF NOT EXISTS summary_en TEXT;

    CREATE TABLE IF NOT EXISTS kospi_heatmap (
        ticker VARCHAR(10) PRIMARY KEY,
        name VARCHAR(100),
        sector VARCHAR(40),
        market_cap BIGINT,
        change_pct NUMERIC(6,2),
        close_price BIGINT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    """

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

def ensure_user(user_key):
    sql = "INSERT INTO users (user_key) VALUES (%s) ON CONFLICT (user_key) DO NOTHING"
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_key,))

def get_watchlist(user_key):
    sql = "SELECT corp_name_kr FROM watchlist WHERE user_key = %s ORDER BY created_at"
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_key,))
            return [row['corp_name_kr'] for row in cur.fetchall()]

def add_to_watchlist(user_key, corp_name_kr):
    sql = """
        INSERT INTO watchlist (user_key, corp_name_kr)
        VALUES (%s, %s)
        ON CONFLICT (user_key, corp_name_kr) DO NOTHING
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_key, corp_name_kr))

def remove_from_watchlist(user_key, corp_name_kr):
    sql = "DELETE FROM watchlist WHERE user_key = %s AND corp_name_kr = %s"
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_key, corp_name_kr))

def save_search_log(keyword):
    sql = """
        INSERT INTO search_logs
        (user_key, ip_address, user_agent, keyword)
        VALUES (%s, %s, %s, %s)
    """

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                get_user_key(),
                request.remote_addr,
                request.headers.get("User-Agent", ""),
                keyword
            ))


def save_disclosure_result(item):
    sql = """
        INSERT INTO disclosure_results
        (user_key, corp_name, stock_code, rcept_no, report_nm, rcept_dt)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_key, rcept_no)
        DO UPDATE SET
            corp_name = EXCLUDED.corp_name,
            stock_code = EXCLUDED.stock_code,
            report_nm = EXCLUDED.report_nm,
            rcept_dt = EXCLUDED.rcept_dt
    """

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                get_user_key(),
                item.get("corp_name"),
                item.get("stock_code"),
                item.get("rcept_no"),
                item.get("report_nm"),
                item.get("rcept_dt")
            ))


def save_interpretation(data, summary, price_info):
    price = None
    change_rate = None
    volume = None

    if price_info:
        price = price_info.get("price")
        change_rate = price_info.get("change")
        volume = price_info.get("volume")

    sql = """
        INSERT INTO disclosure_interpretations
        (user_key, corp_name, stock_code, rcept_no, report_nm, summary, price, change_rate, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                get_user_key(),
                data.get("corp_name"),
                data.get("stock_code"),
                data.get("rcept_no"),
                data.get("report_nm"),
                summary,
                price,
                change_rate,
                volume
            ))


def get_cached_interpretation(rcept_no):
    """A GPT summary for a given filing never changes, so once anyone has
    interpreted a filing we can serve it to everyone else for free instead
    of paying for another OpenAI call."""
    sql = """
        SELECT summary, price, change_rate, volume
        FROM disclosure_interpretations
        WHERE rcept_no = %s
        ORDER BY created_at DESC
        LIMIT 1
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (rcept_no,))
            row = cur.fetchone()
    if not row:
        return None

    price_info = None
    if row['price'] is not None:
        price = float(row['price'])
        change = float(row['change_rate']) if row['change_rate'] is not None else 0.0
        volume = int(row['volume']) if row['volume'] is not None else 0
        arrow = '▲' if change > 0 else '▼' if change < 0 else '-'
        price_info = {
            'price': price, 'change': change, 'volume': volume,
            'price_str': f'{price:,.0f} KRW ({arrow}{abs(change):.2f}%)',
            'volume_str': f'{volume:,} shares',
        }
    return {'summary': row['summary'], 'price_info': price_info}


def get_cached_video_summary(video_id):
    """A GPT summary for a given video never changes, so once anyone has
    summarized a video we can serve it to everyone else for free instead
    of paying for another OpenAI call."""
    sql = "SELECT title, title_en, summary, summary_en FROM video_summaries WHERE video_id = %s"
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (video_id,))
            row = cur.fetchone()
    if not row or not row['summary_en']:
        return None
    return {
        'title':      row['title'],
        'title_en':   row['title_en'],
        'summary':    row['summary'],
        'summary_en': row['summary_en'],
    }

def save_video_summary(video_id, channel_name, title, title_en, published_at, summary, summary_en):
    # If a prior write left this row with an empty/NULL summary_en (e.g. GPT's
    # JSON omitted the key), a later call with a good summary should repair
    # it in place rather than silently discarding the fix.
    sql = """
        INSERT INTO video_summaries (video_id, channel_name, title, title_en, published_at, summary, summary_en)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id) DO UPDATE SET
            title_en = EXCLUDED.title_en, summary = EXCLUDED.summary, summary_en = EXCLUDED.summary_en
        WHERE video_summaries.summary_en IS NULL OR video_summaries.summary_en = ''
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (video_id, channel_name, title, title_en, published_at, summary, summary_en))

def get_rows_missing_english():
    sql = "SELECT video_id, channel_name, title, summary FROM video_summaries WHERE summary_en IS NULL OR summary_en = ''"
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

def update_video_summary_en(video_id, title_en, summary_en):
    sql = "UPDATE video_summaries SET title_en = %s, summary_en = %s WHERE video_id = %s"
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (title_en, summary_en, video_id))


def download_corp_codes():
    resp = http.get('https://opendart.fss.or.kr/api/corpCode.xml',
                    params={'crtfc_key': DART_API_KEY}, timeout=30)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open('CORPCODE.xml') as f:
            tree = ET.parse(f)
    name_map, ticker_map = {}, {}
    for item in tree.getroot().findall('list'):
        name  = item.findtext('corp_name')
        code  = item.findtext('corp_code')
        stock = (item.findtext('stock_code') or '').strip()
        if name and code:
            name_map[name] = code
        if name and stock:
            ticker_map[stock] = name
    return name_map, ticker_map

def load_corp_codes():
    """Returns (name_map, ticker_map): DART official Korean name -> corp_code,
    and stock ticker -> DART official Korean name (listed companies only)."""
    cache_fresh = (
        os.path.exists(CORP_CACHE_FILE) and os.path.exists(TICKER_CACHE_FILE)
        and (time.time() - os.path.getmtime(CORP_CACHE_FILE)) / 86400 < CACHE_MAX_DAYS
    )
    if cache_fresh:
        with open(CORP_CACHE_FILE, 'r', encoding='utf-8') as f:
            name_map = json.load(f)
        with open(TICKER_CACHE_FILE, 'r', encoding='utf-8') as f:
            ticker_map = json.load(f)
        return name_map, ticker_map
    name_map, ticker_map = download_corp_codes()
    with open(CORP_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(name_map, f, ensure_ascii=False)
    with open(TICKER_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(ticker_map, f, ensure_ascii=False)
    return name_map, ticker_map

def resolve_via_yahoo(query):
    """Look up an English company name on Yahoo Finance and return a Korean-exchange
    ticker (e.g. '005930') if one of the top matches is listed on KOSPI/KOSDAQ."""
    try:
        r = http.get(
            'https://query2.finance.yahoo.com/v1/finance/search',
            params={'q': query, 'quotesCount': 8, 'newsCount': 0},
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=8,
        )
        for q in r.json().get('quotes', []):
            symbol = q.get('symbol', '')
            if symbol.endswith('.KS') or symbol.endswith('.KQ'):
                return symbol.split('.')[0]
    except Exception:
        pass
    return None

def resolve_from_dataset(raw_name):
    """Fast, complete lookup against the prebuilt listed-companies dataset
    (Korean name, DART official English name, or ticker). Covers every
    company a user could pick from the search dropdown."""
    key = raw_name.strip()
    if key in _dataset_by_kr:
        return key
    if key in _dataset_by_ticker:
        return _dataset_by_ticker[key]['name_kr']
    key_up = key.upper()
    for c in _companies_dataset:
        if c['name_en'] and c['name_en'].upper() == key_up:
            return c['name_kr']
    return None

def resolve_company_name(raw_name, name_map, ticker_map, eng_name_map):
    """Accepts a DART Korean name, a 6-digit ticker, or an English company name,
    and returns the matching DART Korean name, or None if nothing matches.
    Preference order: prebuilt listed-companies dataset (fast, has real
    English names) -> full DART name list -> ticker -> cached English name
    from a past Yahoo lookup -> live Yahoo Finance search (cold-start
    fallback for a company not in the dataset, e.g. unlisted or newly listed)."""
    from_dataset = resolve_from_dataset(raw_name)
    if from_dataset:
        return from_dataset

    name_map_ci = {k.upper(): k for k in name_map}

    matched = name_map_ci.get(raw_name.upper())
    if matched:
        return matched

    ticker = raw_name.strip()
    if ticker.isdigit() and ticker in ticker_map:
        return ticker_map[ticker]

    cached = eng_name_map.get(raw_name.upper())
    if cached:
        return cached

    yahoo_ticker = resolve_via_yahoo(raw_name)
    if yahoo_ticker and yahoo_ticker in ticker_map:
        return ticker_map[yahoo_ticker]

    return None

def fetch_company_info(corp_code):
    """One DART call gives us both the stock ticker and DART's own official
    English company name (corp_name_eng) - more reliable than guessing via
    a third-party search."""
    r = http.get('https://opendart.fss.or.kr/api/company.json',
                 params={'crtfc_key': DART_API_KEY, 'corp_code': corp_code}, timeout=10)
    d = r.json()
    if d.get('status') != '000':
        return {'stock_code': None, 'eng_name': None}
    return {
        'stock_code': (d.get('stock_code') or '').strip() or None,
        'eng_name':   (d.get('corp_name_eng') or '').strip() or None,
    }

def load_json_cache(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_json_cache(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

def get_or_fetch_company_info(matched_name, corp_code, stock_cache, eng_name_map):
    """Returns {'stock_code', 'eng_name'} for a DART-matched Korean company name,
    using the on-disk cache when available. Fetches from DART and updates both
    caches (in place) on a cache miss. Caller is responsible for persisting."""
    info = stock_cache.get(matched_name)
    if info is None:
        info = fetch_company_info(corp_code)
        stock_cache[matched_name] = info
        if info.get('eng_name'):
            eng_name_map[info['eng_name'].upper()] = matched_name
    return info

def english_company_name(corp_name_kr):
    """Best-effort display name for the filings list: DART's official English
    name from the prebuilt dataset, falling back to the Korean name for
    unlisted companies the dataset doesn't cover."""
    c = _dataset_by_kr.get(corp_name_kr)
    if c and c['name_en']:
        return c['name_en']
    return corp_name_kr

# DART report titles are built from a fairly bounded, repetitive vocabulary
# (report type + an optional parenthetical event). Longest phrases first so
# multi-word terms get matched before their shorter substrings.
REPORT_TITLE_DICT = [
    ('주요사항보고서', 'Material Matters Report'),
    ('사업보고서', 'Business Report'),
    ('반기보고서', 'Semiannual Report'),
    ('분기보고서', 'Quarterly Report'),
    ('연결감사보고서', 'Consolidated Audit Report'),
    ('감사보고서', 'Audit Report'),
    ('증권신고서', 'Securities Registration Statement'),
    ('투자설명서', 'Prospectus'),
    ('일괄신고추가서류', 'Shelf Registration Supplement'),
    ('일괄신고서', 'Shelf Registration Statement'),
    ('정정신고서', 'Amended Registration Statement'),
    ('주식등의대량보유상황보고서', 'Report on Substantial Shareholding of Stocks, etc.'),
    ('임원ㆍ주요주주특정증권등소유상황보고서', "Report on Officers'/Major Shareholders' Ownership of Specific Securities"),
    ('임원ㆍ주요주주 특정증권등 소유상황보고서', "Report on Officers'/Major Shareholders' Ownership of Specific Securities"),
    ('기업설명회', 'IR Session'),
    ('안내공시', 'Guidance Disclosure'),
    ('자율공시안내', 'Voluntary Disclosure Guidance'),
    ('개최', 'Held'),
    ('정기주주총회소집공고', 'Notice of Annual General Meeting'),
    ('임시주주총회소집공고', 'Notice of Extraordinary General Meeting'),
    ('주주총회소집결의', "Resolution to Convene Shareholders' Meeting"),
    ('현금ㆍ현물배당결정', 'Cash/In-kind Dividend Decision'),
    ('현금배당결정', 'Cash Dividend Decision'),
    ('무상증자결정', 'Bonus Issue Decision'),
    ('유상증자결정', 'Rights Offering Decision'),
    ('유무상증자결정', 'Rights Offering and Bonus Issue Decision'),
    ('전환사채권발행결정', 'Convertible Bond Issuance Decision'),
    ('신주인수권부사채권발행결정', 'Bond with Warrant Issuance Decision'),
    ('교환사채권발행결정', 'Exchangeable Bond Issuance Decision'),
    ('자기주식취득결정', 'Treasury Stock Acquisition Decision'),
    ('자기주식처분결정', 'Treasury Stock Disposal Decision'),
    ('자기주식취득결과보고서', 'Report on Results of Treasury Stock Acquisition'),
    ('자기주식처분결과보고서', 'Report on Results of Treasury Stock Disposal'),
    ('자기주식취득신탁계약체결결정', 'Treasury Stock Acquisition Trust Contract Decision'),
    ('자기주식취득신탁계약해지결정', 'Treasury Stock Acquisition Trust Contract Termination Decision'),
    ('타법인주식및출자증권양수결정', 'Decision to Acquire Shares/Equity of Another Company'),
    ('타법인주식및출자증권양도결정', 'Decision to Transfer Shares/Equity of Another Company'),
    ('영업양수결정', 'Business Acquisition Decision'),
    ('영업양도결정', 'Business Transfer Decision'),
    ('합병결정', 'Merger Decision'),
    ('분할합병결정', 'Split-Merger Decision'),
    ('분할결정', 'Spin-off Decision'),
    ('주식교환ㆍ이전결정', 'Share Exchange/Transfer Decision'),
    ('해산사유발생', 'Dissolution Event'),
    ('부도발생', 'Default Event'),
    ('은행거래정지', 'Bank Transaction Suspension'),
    ('영업정지', 'Business Suspension'),
    ('회생절차개시신청', 'Rehabilitation Proceedings Filed'),
    ('파산신청', 'Bankruptcy Filing'),
    ('소송등의제기', 'Litigation Filed'),
    ('자산재평가실시결정', 'Asset Revaluation Decision'),
    ('채권은행등의관리절차개시', 'Creditor Bank Management Procedure Initiated'),
    ('채권은행등의관리절차중단', 'Creditor Bank Management Procedure Terminated'),
    ('매출액또는손익구조', 'Change in Sales or Profit/Loss Structure'),
    ('최대주주변경', 'Change of Largest Shareholder'),
    ('최대주주등소유주식변동신고서', "Report on Changes in Largest Shareholder's Holdings"),
    ('대표이사변경', 'CEO Change'),
    ('결정', 'Decision'),
    ('공고', 'Notice'),
    ('신고서', 'Registration Statement'),
    ('보고서', 'Report'),
    ('정정', 'Amendment'),
    ('첨부정정', 'Attachment Amendment'),
    ('기재정정', 'Content Correction'),
    ('자율공시', 'Voluntary Disclosure'),
    ('일반', 'General'),
    ('약식', 'Abbreviated'),
    ('기타', 'Other'),
    # common [bracket] prefixes DART puts in front of a title
    ('기재정정', 'Content Correction'),
    ('첨부추가', 'Attachment Added'),
    ('첨부정정', 'Attachment Correction'),
    ('발행조건확정', 'Issuance Terms Finalized'),
    ('제출연기', 'Filing Deferred'),
    ('자율공시', 'Voluntary Disclosure'),
    ('공정공시', 'Fair Disclosure'),
    ('연장', 'Extended'),
    ('정정신고(발행조건확정)', 'Amended Filing (Issuance Terms Finalized)'),
    ('해외증권예탁증권관련', 'Related to Overseas Depositary Receipts'),
    ('효력발생안내', 'Effectiveness Notice'),
    ('특정증권등의소유상황보고서', 'Report on Ownership of Specific Securities'),
    ('연결재무제표기준영업(잠정)실적', '(Preliminary) Consolidated Operating Results'),
    ('영업(잠정)실적', '(Preliminary) Operating Results'),
]

KOREAN_CHAR_RE = re.compile(r'[가-힣]')

def _translate_segment(segment):
    """Translate one title segment (the base title, or one parenthetical part).
    Only returns a translation if it fully covers the segment - a half-translated
    mix of Korean and English (e.g. '자기주식처분결과Report') is worse than
    leaving the whole segment in Korean, so we discard partial matches."""
    result = segment
    for kr, en in REPORT_TITLE_DICT:
        if kr in result:
            result = result.replace(kr, en)
    return result if not KOREAN_CHAR_RE.search(result) else segment

def translate_report_title(korean_title):
    """Best-effort English rendering of a DART report title, segment by segment
    (base title, then each (parenthetical) or [bracketed] part) so an
    unrecognized compound term doesn't get chopped into a garbled Korean/
    English mix."""
    parts = re.split(r'(\([^()]*\)|\[[^\[\]]*\])', korean_title)
    translated = []
    for part in parts:
        if (part.startswith('(') and part.endswith(')')) or (part.startswith('[') and part.endswith(']')):
            translated.append(part[0] + _translate_segment(part[1:-1]) + part[-1])
        else:
            translated.append(_translate_segment(part))
    return ''.join(translated)

def build_interest_dict(corp_map, names):
    stock_cache   = load_json_cache(STOCK_CACHE_FILE)
    eng_name_map  = load_json_cache(ENG_NAME_CACHE_FILE)
    corp_map_ci = {k.upper(): k for k in corp_map}
    idict, sdict = {}, {}
    before = json.dumps(stock_cache, sort_keys=True)
    for name in names:
        matched = corp_map_ci.get(name.upper())
        if not matched:
            continue
        corp_code = corp_map[matched]
        info = get_or_fetch_company_info(name, corp_code, stock_cache, eng_name_map)
        idict[name] = corp_code
        sdict[name] = info.get('stock_code')
    if json.dumps(stock_cache, sort_keys=True) != before:
        save_json_cache(STOCK_CACHE_FILE, stock_cache)
        save_json_cache(ENG_NAME_CACHE_FILE, eng_name_map)
    return idict, sdict

def get_disclosures(corp_code, days=1):
    end_date   = datetime.today()
    start_date = end_date - timedelta(days=days)
    r = http.get('https://opendart.fss.or.kr/api/list.json',
                 params={'crtfc_key': DART_API_KEY, 'corp_code': corp_code,
                         'bgn_de': start_date.strftime('%Y%m%d'),
                         'end_de': end_date.strftime('%Y%m%d'), 'page_count': 20}, timeout=10)
    d = r.json()
    return d.get('list', []) if d.get('status') == '000' else []

# Standard DART "key account" names we surface in the financial snapshot,
# mapped to a plain-English label. DART reports figures in KRW.
FINANCIAL_ACCOUNTS = {
    '매출액':              'Revenue',
    '영업이익':            'Operating Profit',
    '영업이익(손실)':      'Operating Profit',
    '당기순이익':          'Net Income',
    '당기순이익(손실)':    'Net Income',
    '자산총계':            'Total Assets',
    '부채총계':            'Total Liabilities',
    '자본총계':            'Total Equity',
}
FINANCIAL_ACCOUNT_ORDER = ['Revenue', 'Operating Profit', 'Net Income', 'Total Assets', 'Total Liabilities', 'Total Equity']

def format_krw(amount_str):
    """DART amounts come as comma-formatted won strings; render them in a
    compact trillion/billion/million form that's easier for a reader to
    scan than a 15-digit number."""
    try:
        n = int(str(amount_str).replace(',', ''))
    except (ValueError, TypeError):
        return amount_str
    sign = '-' if n < 0 else ''
    n = abs(n)
    if n >= 1_000_000_000_000:
        return f'{sign}{n / 1_000_000_000_000:.2f}T KRW'
    if n >= 1_000_000_000:
        return f'{sign}{n / 1_000_000_000:.2f}B KRW'
    if n >= 1_000_000:
        return f'{sign}{n / 1_000_000:.1f}M KRW'
    return f'{sign}{n:,} KRW'

def fetch_financial_snapshot(corp_code):
    """DART's key-account API returns the current + prior 2 fiscal years'
    headline figures in one call. Annual reports (reprt_code 11011) for a
    given year aren't filed until ~March of the following year, so we try
    the most recent year first and fall back a year if that one 404s."""
    current_year = datetime.today().year
    for year in (current_year - 1, current_year - 2):
        try:
            r = http.get('https://opendart.fss.or.kr/api/fnlttSinglAcnt.json',
                         params={'crtfc_key': DART_API_KEY, 'corp_code': corp_code,
                                 'bsns_year': str(year), 'reprt_code': '11011'}, timeout=15)
            d = r.json()
        except Exception:
            continue
        if d.get('status') != '000':
            continue

        items = d.get('list', [])
        chosen = [it for it in items if it.get('fs_div') == 'CFS'] or items

        accounts = {}
        for it in chosen:
            label = FINANCIAL_ACCOUNTS.get(it.get('account_nm'))
            if not label or label in accounts:
                continue
            accounts[label] = {
                'current':       format_krw(it.get('thstrm_amount')),
                'current_period':  it.get('thstrm_nm'),
                'prior':         format_krw(it.get('frmtrm_amount')),
                'prior_period':    it.get('frmtrm_nm'),
                'prior2':        format_krw(it.get('bfefrmtrm_amount')),
                'prior2_period':   it.get('bfefrmtrm_nm'),
            }
        if accounts:
            return {
                'fiscal_year': year,
                'consolidated': bool([it for it in items if it.get('fs_div') == 'CFS']),
                'accounts': accounts,
            }
    return None

def get_stock_price(stock_code):
    """KIS API는 해외 서버에서 접근 제한이 있어 야후 파이낸스로 대체. 코스피(.KS)→코스닥(.KQ) 순으로 시도."""
    if not stock_code:
        return None
    for suffix in ('.KS', '.KQ'):
        try:
            hist = yf.Ticker(f'{stock_code}{suffix}').history(period='5d')
            if hist.empty:
                continue
            price      = hist['Close'].iloc[-1]
            volume     = int(hist['Volume'].iloc[-1])
            prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else price
            change     = (price - prev_close) / prev_close * 100 if prev_close else 0.0
            arrow      = '▲' if change > 0 else '▼' if change < 0 else '-'
            return {'price_str': f'{price:,.0f} KRW ({arrow}{abs(change):.2f}%)',
                    'volume_str': f'{volume:,} shares',
                    'price': float(price), 'change': float(change), 'volume': volume}
        except Exception:
            continue
    return None

def fetch_disclosure_text(rcept_no, max_chars=4000):
    try:
        r = http.get('https://opendart.fss.or.kr/api/document.xml',
                     params={'crtfc_key': DART_API_KEY, 'rcept_no': rcept_no}, timeout=15)
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            xml_files = [f for f in z.namelist() if f.endswith('.xml')]
            if not xml_files:
                return None
            with z.open(xml_files[0]) as f:
                raw = f.read().decode('utf-8', errors='ignore')
        text = re.sub(r'<[^>]+>', ' ', raw)
        return re.sub(r'\s+', ' ', text).strip()[:max_chars]
    except:
        return None

def interpret_with_gpt(corp_name, report_name, content, price_info=None):
    price_text = ''
    if price_info:
        price_text = (
            f"\n[Reference] Current stock price: {price_info['price_str']}, "
            f"volume: {price_info['volume_str']} "
            f"(there may be a delay between the filing and this price, so use it only as context)\n"
        )

    rule = (
        "Writing rules: "
        "Respond in English. Keep it under 120 words. Be concise and information-dense. "
        "Write from an investor's perspective: cover the core meaning, positive factors, "
        "negative factors/risks, and what to verify next. "
        "Judge the filing on its own business substance, not on short-term stock price moves. "
        "Clearly flag anything uncertain as an inference."
    )

    if content:
        prompt = (
            f"The following is the full text of a DART filing by '{corp_name}' titled '{report_name}'."
            f"{price_text}\n"
            f"{rule}\n\n"
            f"{content}"
        )
    else:
        prompt = (
            f"DART filing title: '{corp_name}' - '{report_name}'"
            f"{price_text}\n"
            f"{rule}"
        )

    resp = openai_client.chat.completions.create(
        model='gpt-4o-mini',
        messages=[{'role': 'user', 'content': prompt}],
        max_tokens=500,
        temperature=0.2
    )
    return resp.choices[0].message.content

# ── domestic market issues (YouTube channel summaries) ──
# channel_name -> YouTube channel ID. Add more channels here later; the
# RSS feed + transcript flow below works for any of them unchanged.
MARKET_CHANNELS = {
    '슈카월드':     'UCsJ6RuBiTVWRX156FVbeaGg',
    '삼프로TV':     'UChlv4GSd7OQl3js-jkLOnFA',
    '김작가TV':     'UCvil4OAt-zShzkKHsg9EQAw',
    '묘수의 관점':  'UCLRXZRRlYr3mLxHlnp5iKMw',
}
MAX_VIDEOS_PER_CHANNEL = 10

YT_FEED_NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'yt':   'http://www.youtube.com/xml/schemas/2015',
}

def fetch_channel_videos(channel_id, max_results=10):
    """Latest uploads for a channel via YouTube's public RSS feed - no API
    key or quota needed, unlike the YouTube Data API."""
    r = http.get('https://www.youtube.com/feeds/videos.xml',
                 params={'channel_id': channel_id}, timeout=10)
    root = ET.fromstring(r.content)
    videos = []
    for entry in root.findall('atom:entry', YT_FEED_NS)[:max_results]:
        video_id = entry.findtext('yt:videoId', namespaces=YT_FEED_NS)
        if not video_id:
            continue
        videos.append({
            'video_id':     video_id,
            'title':        entry.findtext('atom:title', namespaces=YT_FEED_NS),
            'published_at': entry.findtext('atom:published', namespaces=YT_FEED_NS),
            'thumbnail':    f'https://i.ytimg.com/vi/{video_id}/mqdefault.jpg',
            'url':          f'https://www.youtube.com/watch?v={video_id}',
        })
    return videos

def fetch_video_transcript(video_id, max_chars=6000):
    """Korean (falling back to English) transcript text, or None if the
    video has no captions available - the GPT summary falls back to the
    title alone in that case."""
    try:
        transcript = YouTubeTranscriptApi().fetch(video_id, languages=['ko', 'en'])
        text = ' '.join(seg['text'] for seg in transcript.to_raw_data())
        return text[:max_chars] if text.strip() else None
    except Exception:
        return None

def summarize_video_bilingual(channel_name, title, transcript):
    """One GPT call produces an English title + English summary (the app's
    default reading language) plus a Korean summary (for the language
    switch), instead of two separate calls. Returns
    {'title_en', 'summary_en', 'summary_ko'}."""
    rule = (
        "You are summarizing a Korean economics/current-affairs YouTube video for "
        "an investor-focused market issues feed. Respond with a single JSON object "
        'with exactly three keys: "title_en" (a natural English rendering of the '
        'video title, not a literal word-for-word translation), "summary_en" (an '
        "English summary, under 120 words, investor's-eye view: core content, "
        'positive factors, risks, and what to verify next), and "summary_ko" (a '
        "Korean summary of the same content, around 300 characters, same "
        "investor's-eye structure). Flag anything uncertain or speculative as such "
        "in both summaries. Judge the video on its own content, not on short-term "
        "market moves."
    )
    if transcript:
        prompt = f"Channel: {channel_name}\nTitle: {title}\n\n{rule}\n\n[Transcript]\n{transcript}"
    else:
        prompt = (
            f"Channel: {channel_name}\nTitle: {title}\n\n{rule}\n\n"
            "(No transcript is available for this video - only the title. Base the "
            "summaries on the title alone and note that they're inferred.)"
        )

    resp = openai_client.chat.completions.create(
        model='gpt-4o-mini',
        response_format={'type': 'json_object'},
        messages=[{'role': 'user', 'content': prompt}],
        max_tokens=800,
        temperature=0.3
    )
    data = json.loads(resp.choices[0].message.content)
    return {
        'title_en':   data.get('title_en') or title,
        'summary_en': data.get('summary_en', ''),
        'summary_ko': data.get('summary_ko', ''),
    }

def translate_summary_to_english(channel_name, title_ko, summary_ko):
    """Cheap follow-up call for rows that only have the Korean fields (e.g.
    ingested before the English-first switch) - translates the existing
    title/summary instead of re-fetching the transcript."""
    prompt = (
        f"Channel: {channel_name}\nKorean title: {title_ko}\nKorean summary: {summary_ko}\n\n"
        'Respond with a JSON object with two keys: "title_en" (a natural English '
        'rendering of the title, not word-for-word) and "summary_en" (an English '
        "rendering of the summary, same content and investor's-eye structure, "
        "under 120 words)."
    )
    resp = openai_client.chat.completions.create(
        model='gpt-4o-mini',
        response_format={'type': 'json_object'},
        messages=[{'role': 'user', 'content': prompt}],
        max_tokens=500,
        temperature=0.3
    )
    data = json.loads(resp.choices[0].message.content)
    return {'title_en': data.get('title_en') or title_ko, 'summary_en': data.get('summary_en', '')}

# ── KOSPI market-cap heatmap ────────────────────────────
KOSPI_HEATMAP_TOP_N = 80

SECTOR_CHOICES = [
    'Technology', 'Consumer Cyclical', 'Consumer Staples', 'Healthcare',
    'Financials', 'Industrials', 'Materials', 'Energy',
    'Communication Services', 'Utilities', 'Real Estate',
]

# Cheap correction pass for company-name suffixes GPT sometimes gets wrong
# (e.g. it tends to bucket any "holding company" as Financials regardless of
# the group's actual business). Checked in order, first match wins.
SECTOR_KEYWORDS = [
    ('조선해양', 'Industrials'), ('중공업', 'Industrials'), ('조선', 'Industrials'),
    ('해운', 'Industrials'), ('항공우주', 'Industrials'), ('건설', 'Industrials'),
    ('은행', 'Financials'), ('증권', 'Financials'), ('생명', 'Financials'),
    ('화재', 'Financials'), ('손해보험', 'Financials'), ('카드', 'Financials'),
    ('캐피탈', 'Financials'), ('금융지주', 'Financials'),
    ('제약', 'Healthcare'), ('바이오', 'Healthcare'), ('헬스케어', 'Healthcare'),
    ('반도체', 'Technology'), ('전자', 'Technology'), ('소프트웨어', 'Technology'),
    ('화학', 'Materials'), ('제철', 'Materials'), ('철강', 'Materials'),
    ('통신', 'Communication Services'),
    ('전력', 'Utilities'), ('가스', 'Utilities'),
]

# A handful of well-known tickers whose name carries no industry-indicating
# keyword (pure group holding companies) but where GPT's default guess
# ("Financials", since it's a holding structure) is wrong.
SECTOR_TICKER_OVERRIDES = {
    '267250': 'Industrials',  # HD현대 - shipbuilding/machinery group holding co
}

def apply_sector_keyword_overrides(ticker, name, gpt_sector):
    if ticker in SECTOR_TICKER_OVERRIDES:
        return SECTOR_TICKER_OVERRIDES[ticker]
    for keyword, sector in SECTOR_KEYWORDS:
        if keyword in name:
            return sector
    return gpt_sector

def fetch_kospi_top_stocks(top_n=KOSPI_HEATMAP_TOP_N):
    """Top-N KOSPI stocks by market cap, with an industry-description hint
    for sector classification. FinanceDataReader is imported here (not at
    module load) since only this batch-only function needs it - the
    always-running Flask process never calls it."""
    import FinanceDataReader as fdr
    kospi = fdr.StockListing('KOSPI')
    desc = fdr.StockListing('KRX-DESC')[['Code', 'Industry']]
    merged = kospi.merge(desc, on='Code', how='left')
    top = merged.sort_values('Marcap', ascending=False).head(top_n)
    return [{
        'ticker':        r['Code'],
        'name':          r['Name'],
        'market_cap':    int(r['Marcap']),
        'change_pct':    float(r['ChagesRatio']),
        'close_price':   int(r['Close']),
        'industry_hint': r['Industry'] if isinstance(r['Industry'], str) else '',
    } for _, r in top.iterrows()]

def classify_sectors(stocks):
    """One GPT call classifies every given stock at once. Returns
    {ticker: sector}. Caller should only pass tickers not already
    classified in the DB - a stock's sector essentially never changes, so
    there's no need to re-spend a call on it once it's cached."""
    lines = [f"{s['ticker']}: {s['name']} ({s['industry_hint'] or 'N/A'})" for s in stocks]
    prompt = (
        'Classify each Korean-listed KOSPI company below into exactly one of these '
        'sectors: ' + ', '.join(SECTOR_CHOICES) + '.\n\n'
        'Use your own knowledge of the company first - the Korean industry text in '
        'parentheses is often just a generic legal-entity classification (e.g. many '
        'holding companies are filed under a generic "other business" industry code) '
        'and should not override what you know about the company. For a holding '
        'company ("지주", "홀딩스"), classify by its largest underlying operating '
        'business (e.g. a shipbuilding holding company is Industrials, a bank holding '
        'company is Financials) - do not default to Financials just because it is a '
        'holding structure.\n\n'
        'Respond with a JSON object mapping each 6-digit ticker code to its sector '
        'name exactly as spelled above.\n\n' + '\n'.join(lines)
    )
    resp = openai_client.chat.completions.create(
        model='gpt-4o-mini',
        response_format={'type': 'json_object'},
        messages=[{'role': 'user', 'content': prompt}],
        max_tokens=2000,
        temperature=0,
    )
    raw = json.loads(resp.choices[0].message.content)
    name_by_ticker = {s['ticker']: s['name'] for s in stocks}
    result = {}
    for ticker, sector in raw.items():
        if sector not in SECTOR_CHOICES:
            sector = 'Industrials'  # unrecognized label from the model - safe generic bucket
        result[ticker] = apply_sector_keyword_overrides(ticker, name_by_ticker.get(ticker, ''), sector)
    return result

def get_existing_sectors(tickers):
    if not tickers:
        return {}
    sql = "SELECT ticker, sector FROM kospi_heatmap WHERE ticker = ANY(%s) AND sector IS NOT NULL"
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tickers,))
            return {r['ticker']: r['sector'] for r in cur.fetchall()}

def save_kospi_snapshot(stocks_with_sector):
    sql = """
        INSERT INTO kospi_heatmap (ticker, name, sector, market_cap, change_pct, close_price, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (ticker) DO UPDATE SET
            name = EXCLUDED.name,
            sector = COALESCE(EXCLUDED.sector, kospi_heatmap.sector),
            market_cap = EXCLUDED.market_cap, change_pct = EXCLUDED.change_pct,
            close_price = EXCLUDED.close_price, updated_at = CURRENT_TIMESTAMP
    """
    tickers = [s['ticker'] for s in stocks_with_sector]
    with get_db() as conn:
        with conn.cursor() as cur:
            for s in stocks_with_sector:
                cur.execute(sql, (
                    s['ticker'], s['name'], s['sector'],
                    s['market_cap'], s['change_pct'], s['close_price']
                ))
            # Drop anything that fell out of today's top-N so a stale market
            # cap from a stock that's no longer in it can't outrank a
            # current constituent in the ORDER BY market_cap DESC LIMIT.
            cur.execute("DELETE FROM kospi_heatmap WHERE ticker != ALL(%s)", (tickers,))

# ── Flask ──────────────────────────────────────────────
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/companies', methods=['GET'])
def api_get_companies():
    email = require_user_email()
    if not email:
        return jsonify({'error': 'Please sign in with your email first.'}), 401
    try:
        return jsonify(get_watchlist(email))
    except Exception as e:
        return jsonify({'error': f'Could not load your watchlist: {e}'}), 500

@app.route('/api/companies/search')
def api_search_companies():
    if rate_limited(rate_limit_key(), max_calls=60, window_sec=60):
        return jsonify({'error': 'Too many requests. Please slow down.'}), 429
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])
    q_up = q.upper()

    scored = []
    for c in _companies_dataset:
        name_en_up = c['name_en'].upper() if c['name_en'] else ''
        if c['ticker'] == q:
            score = 0
        elif c['name_kr'].startswith(q) or name_en_up.startswith(q_up):
            score = 1
        elif q in c['name_kr'] or q_up in name_en_up or c['ticker'].startswith(q):
            score = 2
        else:
            continue
        # Within a tier, shorter names tend to be the well-known/primary entity
        # (e.g. "SAMSUNG ELECTRONICS CO,.LTD" before "SAMSUNG SPECIAL PURPOSE ACQUISITION...").
        scored.append((score, len(c['name_kr']), c))

    scored.sort(key=lambda t: (t[0], t[1]))
    return jsonify([c for _, _, c in scored[:10]])

@app.route('/api/companies', methods=['POST'])
def api_add_company():
    email = require_user_email()
    if not email:
        return jsonify({'error': 'Please sign in with your email first.'}), 401
    if rate_limited(rate_limit_key(), max_calls=20, window_sec=60):
        return jsonify({'error': 'Too many requests. Please wait a minute and try again.'}), 429

    raw_name = (request.json or {}).get('name', '').strip()
    if not raw_name:
        return jsonify({'error': 'Please enter a company name or ticker.'}), 400

    try:
        ensure_user(email)
        save_search_log(raw_name)
    except Exception as e:
        print(f'ensure_user/save_search_log failed: {e}', flush=True)

    name_map, ticker_map = load_corp_codes()
    stock_cache  = load_json_cache(STOCK_CACHE_FILE)
    eng_name_map = load_json_cache(ENG_NAME_CACHE_FILE)

    matched = resolve_company_name(raw_name, name_map, ticker_map, eng_name_map)
    if not matched:
        return jsonify({
            'error': f'Could not find a DART-listed company matching "{raw_name}". '
                     f'Try the official Korean name or the 6-digit ticker (e.g. 005930).'
        }), 404

    # Cache DART's stock code + official English name now, so future lookups
    # (Korean, English, or ticker) for this company resolve instantly.
    get_or_fetch_company_info(matched, name_map[matched], stock_cache, eng_name_map)
    # Also remember the exact phrase the user typed, so re-typing it later
    # (even if it's not DART's official English name) skips the Yahoo lookup.
    eng_name_map[raw_name.upper()] = matched
    save_json_cache(STOCK_CACHE_FILE, stock_cache)
    save_json_cache(ENG_NAME_CACHE_FILE, eng_name_map)

    try:
        add_to_watchlist(email, matched)
        companies = get_watchlist(email)
    except Exception as e:
        return jsonify({'error': f'Could not save to your watchlist: {e}'}), 500

    return jsonify({'companies': companies, 'resolved': matched})

@app.route('/api/companies/<path:name>', methods=['DELETE'])
def api_del_company(name):
    email = require_user_email()
    if not email:
        return jsonify({'error': 'Please sign in with your email first.'}), 401
    try:
        remove_from_watchlist(email, name)
        return jsonify(get_watchlist(email))
    except Exception as e:
        return jsonify({'error': f'Could not update your watchlist: {e}'}), 500

@app.route('/api/companies/<path:name>/financials')
def api_company_financials(name):
    email = require_user_email()
    if not email:
        return jsonify({'error': 'Please sign in with your email first.'}), 401
    if rate_limited(rate_limit_key(), max_calls=20, window_sec=60):
        return jsonify({'error': 'Too many requests. Please wait a minute and try again.'}), 429

    name_map, _ticker_map = load_corp_codes()
    corp_code = name_map.get(name)
    if not corp_code:
        return jsonify({'error': f'Unknown company "{name}".'}), 404

    try:
        snapshot = fetch_financial_snapshot(corp_code)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    if not snapshot:
        return jsonify({'error': 'No financial statement data available for this company yet.'}), 404

    snapshot['account_order'] = FINANCIAL_ACCOUNT_ORDER
    return jsonify(snapshot)

@app.route('/api/disclosures')
def api_disclosures():
    email = require_user_email()
    if not email:
        return jsonify({'error': 'Please sign in with your email first.'}), 401
    if rate_limited(rate_limit_key(), max_calls=15, window_sec=60):
        return jsonify({'error': 'Too many requests. Please wait a minute and try again.'}), 429
    try:
        my_companies = get_watchlist(email)
    except Exception as e:
        return jsonify({'error': f'Could not load your watchlist: {e}'}), 500
    if not my_companies:
        return jsonify([])
    try:
        name_map, _ticker_map = load_corp_codes()
        idict, sdict = build_interest_dict(name_map, my_companies)
        items = []
        for corp_name, corp_code in idict.items():
            stock_code = sdict.get(corp_name)
            display_name = english_company_name(corp_name)
            for d in get_disclosures(corp_code, days=90)[:15]:
                items.append({
                    'corp_name':     display_name,
                    'corp_name_kr':  corp_name,
                    'stock_code':    stock_code,
                    'rcept_no':      d['rcept_no'],
                    'report_nm':     translate_report_title(d['report_nm']),
                    'report_nm_kr':  d['report_nm'],
                    'rcept_dt':      d['rcept_dt'],
                })
        items.sort(key=lambda x: x['rcept_dt'], reverse=True)
        return jsonify(items)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/interpret', methods=['POST'])
def api_interpret():
    data = request.json or {}
    rcept_no = data.get('rcept_no')

    try:
        cached = get_cached_interpretation(rcept_no) if rcept_no else None
    except Exception as e:
        print(f'get_cached_interpretation failed: {e}', flush=True)
        cached = None
    if cached:
        return jsonify(cached)

    # Only rate-limit the expensive path (an actual OpenAI call). Cache hits
    # above are free, so they don't count against the limit.
    if rate_limited(rate_limit_key(), max_calls=15, window_sec=60):
        return jsonify({'error': 'Too many requests. Please wait a minute and try again.'}), 429

    try:
        price_info = get_stock_price(data.get('stock_code'))
        content    = fetch_disclosure_text(rcept_no)
        summary    = interpret_with_gpt(
            data.get('corp_name'),
            data.get('report_nm'),
            content,
            price_info
        )

        try:
            save_interpretation(data, summary, price_info)
        except Exception as e:
            print(f'save_interpretation failed: {e}', flush=True)

        return jsonify({'summary': summary, 'price_info': price_info})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/market-issues')
def api_market_issues():
    """Reads the pre-ingested video+summary catalog straight from Postgres -
    no live YouTube/OpenAI calls here. A scheduled batch job
    (market_issues_job.py) is what keeps this table filled; see its
    docstring for how it's meant to be run."""
    email = require_user_email()
    if not email:
        return jsonify({'error': 'Please sign in with your email first.'}), 401
    try:
        # A plain "most recent 30 overall" would let a high-frequency channel
        # (multiple uploads/day) crowd out a low-frequency one entirely,
        # so cap it per channel instead and merge by date for display.
        sql = """
            SELECT video_id, channel_name, title, title_en, published_at, summary, summary_en
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY channel_name ORDER BY published_at DESC) AS rn
                FROM video_summaries
                WHERE summary_en IS NOT NULL
            ) ranked
            WHERE rn <= 8
            ORDER BY published_at DESC
        """
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        items = [{
            'video_id':     r['video_id'],
            'channel_name': r['channel_name'],
            'title':        r['title'],
            'title_en':     r['title_en'],
            'published_at': r['published_at'],
            'summary':      r['summary'],
            'summary_en':   r['summary_en'],
            'thumbnail':    f"https://i.ytimg.com/vi/{r['video_id']}/mqdefault.jpg",
            'url':          f"https://www.youtube.com/watch?v={r['video_id']}",
        } for r in rows]
        return jsonify(items)
    except Exception as e:
        return jsonify({'error': f'Could not load market issues: {e}'}), 500

@app.route('/api/market-issues/summarize', methods=['POST'])
def api_market_issues_summarize():
    """Fallback only: the batch job normally pre-generates every summary,
    so the frontend serves `summary` straight from /api/market-issues and
    never calls this in the common case. This exists for a video the job
    hasn't gotten to yet (e.g. published since the last run)."""
    email = require_user_email()
    if not email:
        return jsonify({'error': 'Please sign in with your email first.'}), 401

    data = request.json or {}
    video_id = data.get('video_id')
    if not video_id:
        return jsonify({'error': 'Missing video_id.'}), 400

    try:
        cached = get_cached_video_summary(video_id)
    except Exception as e:
        print(f'get_cached_video_summary failed: {e}', flush=True)
        cached = None
    if cached:
        return jsonify(cached)

    # Only rate-limit the expensive path (transcript fetch + OpenAI call).
    if rate_limited(rate_limit_key(), max_calls=10, window_sec=60):
        return jsonify({'error': 'Too many requests. Please wait a minute and try again.'}), 429

    try:
        transcript = fetch_video_transcript(video_id)
        result = summarize_video_bilingual(
            data.get('channel_name', ''),
            data.get('title', ''),
            transcript
        )
        title = data.get('title')
        try:
            save_video_summary(
                video_id,
                data.get('channel_name'),
                title,
                result['title_en'],
                data.get('published_at'),
                result['summary_ko'],
                result['summary_en']
            )
        except Exception as e:
            print(f'save_video_summary failed: {e}', flush=True)
        return jsonify({
            'title':      title,
            'title_en':   result['title_en'],
            'summary':    result['summary_ko'],
            'summary_en': result['summary_en'],
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/kospi-heatmap')
def api_kospi_heatmap():
    """Reads the daily KOSPI snapshot straight from Postgres - like the
    market-issues tab, this never calls out to FinanceDataReader/OpenAI
    itself. A scheduled batch job (kospi_heatmap_job.py) keeps the table
    fresh."""
    email = require_user_email()
    if not email:
        return jsonify({'error': 'Please sign in with your email first.'}), 401
    try:
        sql = """
            SELECT ticker, name, sector, market_cap, change_pct, close_price, updated_at
            FROM kospi_heatmap
            ORDER BY market_cap DESC
            LIMIT %s
        """
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (KOSPI_HEATMAP_TOP_N,))
                rows = cur.fetchall()
        items = [{
            'ticker':      r['ticker'],
            'name':        r['name'],
            'name_en':     (_dataset_by_ticker.get(r['ticker']) or {}).get('name_en') or r['name'],
            'sector':      r['sector'],
            'market_cap':  r['market_cap'],
            'change_pct':  float(r['change_pct']) if r['change_pct'] is not None else 0.0,
            'close_price': r['close_price'],
        } for r in rows]
        updated_at = rows[0]['updated_at'].isoformat() if rows else None
        return jsonify({'items': items, 'updated_at': updated_at})
    except Exception as e:
        return jsonify({'error': f'Could not load KOSPI heatmap: {e}'}), 500

try:
    init_db()
    print("DB init complete", flush=True)
except Exception as e:
    print(f"DB init failed: {e}", flush=True)


if __name__ == '__main__':
    app.run(debug=False, port=5000, threaded=True)
