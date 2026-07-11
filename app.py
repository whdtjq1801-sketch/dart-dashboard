import os, json, time, re, zipfile, io
import psycopg2
from psycopg2.extras import RealDictCursor
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template
import requests as http
import yfinance as yf
from openai import OpenAI
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

# ── global state ───────────────────────────────────────
_companies    = []

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

    """

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

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

# ── Flask ──────────────────────────────────────────────
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/companies', methods=['GET'])
def api_get_companies():
    return jsonify(_companies)

@app.route('/api/companies/search')
def api_search_companies():
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
    raw_name = (request.json or {}).get('name', '').strip()
    if not raw_name:
        return jsonify({'error': 'Please enter a company name or ticker.'}), 400

    try:
        save_search_log(raw_name)
    except Exception as e:
        print(f'save_search_log failed: {e}', flush=True)

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

    if matched not in _companies:
        _companies.append(matched)

    return jsonify({'companies': _companies, 'resolved': matched})

@app.route('/api/companies/<path:name>', methods=['DELETE'])
def api_del_company(name):
    if name in _companies:
        _companies.remove(name)
    return jsonify(_companies)

@app.route('/api/disclosures')
def api_disclosures():
    if not _companies:
        return jsonify([])
    try:
        name_map, _ticker_map = load_corp_codes()
        idict, sdict = build_interest_dict(name_map, _companies)
        items = []
        for corp_name, corp_code in idict.items():
            stock_code = sdict.get(corp_name)
            for d in get_disclosures(corp_code, days=90)[:15]:
                items.append({
                    'corp_name':  corp_name,
                    'stock_code': stock_code,
                    'rcept_no':   d['rcept_no'],
                    'report_nm':  d['report_nm'],
                    'rcept_dt':   d['rcept_dt'],
                })
        items.sort(key=lambda x: x['rcept_dt'], reverse=True)
        return jsonify(items)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/interpret', methods=['POST'])
def api_interpret():
    data = request.json or {}
    try:
        price_info = get_stock_price(data.get('stock_code'))
        content    = fetch_disclosure_text(data.get('rcept_no'))
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

try:
    init_db()
    print("DB init complete", flush=True)
except Exception as e:
    print(f"DB init failed: {e}", flush=True)


if __name__ == '__main__':
    app.run(debug=False, port=5000, threaded=True)
