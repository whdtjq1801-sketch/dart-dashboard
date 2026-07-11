"""
DART에 등록된 전체 상장기업(티커 보유)에 대해 한글명/영문명/티커를 미리 수집해
static/companies.json으로 저장한다. 이 파일은 git에 커밋해서 배포 시 그대로
쓰고, 매번 DART를 다시 크롤링하지 않아도 되게 한다.

1회성 스크립트. 완료까지 상장기업 수(~4000개) x 요청당 딜레이만큼 걸린다.
"""
import io
import json
import os
import time
import zipfile
import xml.etree.ElementTree as ET

import requests as http
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))
DART_API_KEY = os.getenv('DART_API_KEY')

OUT_DIR = os.path.join(BASE_DIR, 'static')
OUT_FILE = os.path.join(OUT_DIR, 'companies.json')
PROGRESS_FILE = os.path.join(BASE_DIR, 'build_companies_progress.json')

REQUEST_DELAY_SEC = 0.15


def download_corp_list():
    resp = http.get('https://opendart.fss.or.kr/api/corpCode.xml',
                     params={'crtfc_key': DART_API_KEY}, timeout=30)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open('CORPCODE.xml') as f:
            tree = ET.parse(f)
    listed = []
    for item in tree.getroot().findall('list'):
        name  = item.findtext('corp_name')
        code  = item.findtext('corp_code')
        stock = (item.findtext('stock_code') or '').strip()
        if name and code and stock:
            listed.append({'name_kr': name, 'corp_code': code, 'ticker': stock})
    return listed


def fetch_eng_name(corp_code):
    r = http.get('https://opendart.fss.or.kr/api/company.json',
                 params={'crtfc_key': DART_API_KEY, 'corp_code': corp_code}, timeout=10)
    d = r.json()
    if d.get('status') != '000':
        return None
    return (d.get('corp_name_eng') or '').strip() or None


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_progress(done):
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(done, f, ensure_ascii=False)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    listed = download_corp_list()
    print(f'상장기업(티커 보유) {len(listed)}개 확인. 영문명 수집 시작...')

    done = load_progress()  # corp_code -> eng_name (or "" if lookup failed)

    for i, co in enumerate(listed, 1):
        code = co['corp_code']
        if code not in done:
            try:
                eng = fetch_eng_name(code)
            except Exception as e:
                print(f'  [{i}/{len(listed)}] {co["name_kr"]} 실패: {e}')
                eng = None
            done[code] = eng or ''
            time.sleep(REQUEST_DELAY_SEC)
            if i % 200 == 0:
                save_progress(done)
                print(f'  [{i}/{len(listed)}] 진행 중... (중간 저장)')

    save_progress(done)

    result = []
    for co in listed:
        eng = done.get(co['corp_code']) or None
        result.append({
            'name_kr': co['name_kr'],
            'name_en': eng,
            'ticker':  co['ticker'],
            'corp_code': co['corp_code'],
        })

    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=1)

    with_eng = sum(1 for r in result if r['name_en'])
    print(f'\n완료: {len(result)}개 기업 저장 -> {OUT_FILE} (영문명 있음: {with_eng}개)')


if __name__ == '__main__':
    main()
