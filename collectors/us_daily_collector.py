#!/usr/bin/env python3
"""
US 시장 일별 시세 수집 (yfinance).
기준일 OHLCV·거래대금(volume*close) 수집 후 JSON 배열을 stdout으로 출력.
Spring UsMarketCollectionService에서 이 서비스를 HTTP로 호출해 파싱·TB_DAILY_STOCK(MARKET=US) 저장.

수정주가 정책(ADR 19): 팩터·백테스트 입력은 수정주가만 사용. yfinance history(auto_adjust=True)로
배당·분할 반영된 수정주가를 수집한다.

사용:
  python us_daily_collector.py --bas-dt 2026-01-30
  python us_daily_collector.py --bas-dt 2026-01-30 --symbols AAPL,MSFT,GOOGL

필요: pip install yfinance
"""
import argparse
import json
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any


def fetch_us_daily(bas_dt: str, symbols: List[str]) -> List[Dict[str, Any]]:
    """yfinance로 기준일 US 종목 OHLCV 수집. trdVal = volume * close (달러 거래대금)."""
    try:
        import yfinance as yf
    except ImportError:
        print("yfinance 미설치: pip install yfinance", file=sys.stderr)
        return []

    base = datetime.strptime(bas_dt, "%Y-%m-%d").date()
    start = base
    end = base + timedelta(days=1)

    rows = []
    for symbol in symbols:
        symbol = symbol.strip()
        if not symbol:
            continue
        try:
            ticker = yf.Ticker(symbol)
            # auto_adjust=True: 수정주가(배당·분할 반영). 팩터·백테스트는 수정주가만 사용(ADR 19).
            hist = ticker.history(start=start, end=end, auto_adjust=True)
            if hist is None or hist.empty:
                continue
            for idx, row in hist.iterrows():
                open_p = row.get("Open")
                high_p = row.get("High")
                low_p = row.get("Low")
                close_p = row.get("Close")
                volume = row.get("Volume")
                if close_p is None or (hasattr(close_p, "item") and str(close_p) == "nan"):
                    continue
                try:
                    close_val = float(close_p)
                    vol_val = int(float(volume)) if volume is not None and str(volume) != "nan" else 0
                    trd_val = int(vol_val * close_val) if vol_val and close_val else 0
                except (TypeError, ValueError):
                    vol_val = 0
                    trd_val = 0

                def to_num(x):
                    if x is None or (hasattr(x, "item") and str(x) == "nan"):
                        return None
                    try:
                        return round(float(x), 4)
                    except (TypeError, ValueError):
                        return None

                rows.append({
                    "symbol": symbol,
                    "open": to_num(open_p),
                    "high": to_num(high_p),
                    "low": to_num(low_p),
                    "close": to_num(close_p),
                    "volume": vol_val,
                    "trdVal": trd_val,
                })
                break
        except Exception as e:
            print(f"yfinance {symbol} 오류: {e}", file=sys.stderr)
            continue
    return rows


def main():
    parser = argparse.ArgumentParser(description="US 일별 시세 수집 (yfinance)")
    parser.add_argument("--bas-dt", required=True, help="기준일 YYYY-MM-DD")
    parser.add_argument("--symbols", default="AAPL,MSFT,GOOGL,AMZN,META,TSLA,NVDA,JPM,V,JNJ", help="쉼표 구분 종목 코드")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("symbols 비어 있음", file=sys.stderr)
        sys.exit(1)

    rows = fetch_us_daily(args.bas_dt, symbols)
    print(json.dumps(rows, ensure_ascii=False))


if __name__ == "__main__":
    main()
