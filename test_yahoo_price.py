import sys
import yfinance as yf

if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def get_stock_price_yahoo(stock_code):
    """KIS 대신 Yahoo Finance로 한국 주식 현재가 조회. 코스피(.KS)/코스닥(.KQ) 순으로 시도."""
    for suffix in ('.KS', '.KQ'):
        ticker = yf.Ticker(f'{stock_code}{suffix}')
        hist = ticker.history(period='5d')
        if hist.empty or len(hist) < 1:
            continue
        last_close = hist['Close'].iloc[-1]
        volume = int(hist['Volume'].iloc[-1])
        prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else last_close
        change = (last_close - prev_close) / prev_close * 100 if prev_close else 0.0
        arrow = '▲' if change > 0 else '▼' if change < 0 else '-'
        return {
            'ticker': f'{stock_code}{suffix}',
            'price_str': f'{last_close:,.0f}원 ({arrow}{abs(change):.2f}%)',
            'volume_str': f'{volume:,}주',
            'price': last_close,
            'change': change,
            'volume': volume,
        }
    return None


if __name__ == '__main__':
    code = sys.argv[1] if len(sys.argv) > 1 else '005930'  # 삼성전자
    result = get_stock_price_yahoo(code)
    print(result if result else f'조회 실패: {code}')
