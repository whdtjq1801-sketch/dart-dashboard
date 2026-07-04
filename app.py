import os, json, time, re, zipfile, io, threading
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template, Response
import requests as http
import yfinance as yf
from openai import OpenAI
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

DART_API_KEY       = os.getenv('DART_API_KEY')
OPENAI_API_KEY     = os.getenv('OPENAI_API_KEY')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID')
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_TOKEN  = os.getenv("ADMIN_TOKEN")

SEEN_FILE        = os.path.join(BASE_DIR, 'seen_disclosures.json')
CORP_CACHE_FILE  = os.path.join(BASE_DIR, 'corp_map_cache.json')
STOCK_CACHE_FILE = os.path.join(BASE_DIR, 'stock_code_cache.json')
CACHE_MAX_DAYS   = 7

openai_client = OpenAI(api_key=OPENAI_API_KEY)

# ── global state ───────────────────────────────────────
_companies    = []
_seen         = set()
_monitor_on   = False
_stop_event   = threading.Event()
_logs         = []
_logs_lock    = threading.Lock()
_interval_min = 60

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

def _log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    with _logs_lock:
        _logs.append(f'[{ts}] {msg}')
        if len(_logs) > 50:
            _logs.pop(0)

def load_seen():
    global _seen
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            _seen = set(json.load(f))

def save_seen():
    with open(SEEN_FILE, 'w') as f:
        json.dump(list(_seen), f)

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

    CREATE TABLE IF NOT EXISTS telegram_send_logs (
        id BIGSERIAL PRIMARY KEY,
        user_key VARCHAR(100),
        rcept_no VARCHAR(30),
        corp_name VARCHAR(100),
        report_nm VARCHAR(300),
        sent BOOLEAN,
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


def save_telegram_log(data, sent):
    sql = """
        INSERT INTO telegram_send_logs
        (user_key, rcept_no, corp_name, report_nm, sent)
        VALUES (%s, %s, %s, %s, %s)
    """

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                get_user_key(),
                data.get("rcept_no"),
                data.get("corp_name"),
                data.get("report_nm"),
                sent
            ))


def download_corp_codes():
    resp = http.get('https://opendart.fss.or.kr/api/corpCode.xml',
                    params={'crtfc_key': DART_API_KEY}, timeout=30)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open('CORPCODE.xml') as f:
            tree = ET.parse(f)
    m = {}
    for item in tree.getroot().findall('list'):
        name, code = item.findtext('corp_name'), item.findtext('corp_code')
        if name and code:
            m[name] = code
    return m

def load_corp_codes():
    if os.path.exists(CORP_CACHE_FILE):
        if (time.time() - os.path.getmtime(CORP_CACHE_FILE)) / 86400 < CACHE_MAX_DAYS:
            with open(CORP_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    m = download_corp_codes()
    with open(CORP_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(m, f, ensure_ascii=False)
    return m

def get_stock_code(corp_code):
    r = http.get('https://opendart.fss.or.kr/api/company.json',
                 params={'crtfc_key': DART_API_KEY, 'corp_code': corp_code}, timeout=10)
    d = r.json()
    return d.get('stock_code', '').strip() or None if d.get('status') == '000' else None

def build_interest_dict(corp_map, names):
    stock_cache = {}
    if os.path.exists(STOCK_CACHE_FILE):
        with open(STOCK_CACHE_FILE, 'r', encoding='utf-8') as f:
            stock_cache = json.load(f)
    corp_map_ci = {k.upper(): k for k in corp_map}
    idict, sdict, updated = {}, {}, False
    for name in names:
        matched = corp_map_ci.get(name.upper())
        if not matched:
            continue
        corp_code  = corp_map[matched]
        stock_code = stock_cache.get(name) or get_stock_code(corp_code)
        if name not in stock_cache:
            stock_cache[name] = stock_code
            updated = True
        idict[name] = corp_code
        sdict[name] = stock_code
    if updated:
        with open(STOCK_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(stock_cache, f, ensure_ascii=False)
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
            return {'price_str': f'{price:,.0f}원 ({arrow}{abs(change):.2f}%)',
                    'volume_str': f'{volume:,}주',
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
        price_text = (f"\n[참고] 현재 주가: {price_info['price_str']}, 거래량: {price_info['volume_str']}"
                      f" (공시와 시차가 있으므로 주가는 참고용으로만 활용할 것)\n")
    if content:
        prompt = (f"다음은 '{corp_name}'의 DART 공시 '{report_name}' 원문이야.{price_text}\n"
                  f"공시 내용 자체에 집중해서 200자 내외로 핵심을 요약하고 투자자 관점에서 의미를 설명해줘. "
                  f"주가 등락은 해석에 직접 반영하지 말고, 공시의 본질적 내용과 사업적 의미에 초점을 맞춰줘.\n\n{content}")
    else:
        prompt = (f"DART 공시 제목: '{corp_name}' - '{report_name}'{price_text}\n"
                  f"이 공시의 사업적 의미를 투자자 관점에서 200자 내외로 설명해줘. "
                  f"주가 등락은 해석에 직접 반영하지 말고 공시 내용의 본질에 집중해줘.")
    resp = openai_client.chat.completions.create(
        model='gpt-4o-mini',
        messages=[{'role': 'user', 'content': prompt}],
        max_tokens=300, temperature=0.3)
    return resp.choices[0].message.content

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = http.post(
            f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML'},
            timeout=10)
        return r.ok
    except Exception as e:
        _log(f'텔레그램 전송 실패: {e}')
        return False

def build_telegram_message(corp_name, report_nm, rcept_dt, rcept_no, summary, price_info=None):
    dart_url   = f'https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}'
    price_line = (f'\n주가: {price_info["price_str"]}  |  거래량: {price_info["volume_str"]}'
                  if price_info else '')
    return (f'📢 <b>[{corp_name}] {report_nm}</b>\n'
            f'접수일: {rcept_dt}{price_line}\n\n'
            f'{summary}\n\n'
            f'<a href="{dart_url}">📄 DART 원문 보기</a>')

# ── monitor loop ───────────────────────────────────────
def monitor_loop():
    global _monitor_on
    while not _stop_event.is_set():
        try:
            _log('공시 체크 중...')
            corp_map = load_corp_codes()
            idict, sdict = build_interest_dict(corp_map, _companies)
            for corp_name, corp_code in idict.items():
                new_ones = [d for d in get_disclosures(corp_code, days=1)
                            if d['rcept_no'] not in _seen]
                for d in new_ones:
                    _seen.add(d['rcept_no'])
                    _log(f'📄 [{corp_name}] {d["report_nm"]}')
                    def _notify(cn=corp_name, disc=d):
                        try:
                            sc         = sdict.get(cn)
                            price_info = get_stock_price(sc)
                            content    = fetch_disclosure_text(disc['rcept_no'])
                            summary    = interpret_with_gpt(cn, disc['report_nm'], content, price_info)
                            msg = build_telegram_message(cn, disc['report_nm'], disc['rcept_dt'],
                                                          disc['rcept_no'], summary, price_info)
                            send_telegram(msg)
                        except Exception as te:
                            _log(f'텔레그램 전송 실패: {te}')
                    threading.Thread(target=_notify, daemon=True).start()
            save_seen()
            _log(f'완료. {_interval_min}분 후 재실행.')
        except Exception as e:
            _log(f'❌ 오류: {e}')
        _stop_event.wait(_interval_min * 60)
    _monitor_on = False

# ── Flask ──────────────────────────────────────────────
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/companies', methods=['GET'])
def api_get_companies():
    return jsonify(_companies)

@app.route('/api/companies', methods=['POST'])
def api_add_company():
    name = (request.json or {}).get('name', '').strip()

    if name:
        save_search_log(name)

    if name and name not in _companies:
        _companies.append(name)

    return jsonify(_companies)

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
        corp_map = load_corp_codes()
        idict, sdict = build_interest_dict(corp_map, _companies)
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

        save_interpretation(data, summary, price_info)

        return jsonify({'summary': summary, 'price_info': price_info})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/send_telegram', methods=['POST'])
def api_send_telegram():
    data = request.json or {}
    try:
        msg = build_telegram_message(data.get('corp_name'), data.get('report_nm'),
                                      data.get('rcept_dt'), data.get('rcept_no'),
                                      data.get('summary'), data.get('price_info'))
        return jsonify({'sent': send_telegram(msg)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitor/start', methods=['POST'])
def api_start():
    global _monitor_on, _interval_min
    _interval_min = int((request.json or {}).get('interval', 60))
    if not _monitor_on:
        _monitor_on = True
        _stop_event.clear()
        threading.Thread(target=monitor_loop, daemon=True).start()
    return jsonify({'running': _monitor_on})

@app.route('/api/monitor/stop', methods=['POST'])
def api_stop():
    global _monitor_on
    _stop_event.set()
    _monitor_on = False
    return jsonify({'running': False})

@app.route('/api/status')
def api_status():
    with _logs_lock:
        logs = list(_logs[-8:])
    return jsonify({'running': _monitor_on, 'interval': _interval_min, 'logs': logs})

try:
    init_db()
    print("DB 초기화 완료", flush=True)
except Exception as e:
    print(f"DB 초기화 실패: {e}", flush=True)


if __name__ == '__main__':
    load_seen()
    app.run(debug=False, port=5000, threaded=True)
