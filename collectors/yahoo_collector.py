#!/usr/bin/env python3
"""
Yahoo Finance 수집 스크립트 (Earnings Calendar 등)
수집 결과를 Spring 내부 API POST /api/v1/internal/collected-news 로 전달합니다.

필요 환경변수:
  SPRING_BASE_URL: Spring 서버 URL (예: http://localhost:8080)
  DATA_COLLECTION_INTERNAL_KEY: Spring application.yml의 investment.data.internal-api-key 와 동일

선택: pip install yfinance  후 Earnings Calendar 수집 가능
"""
import os
import sys
import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import List, Dict, Any

SPRING_BASE_URL = os.environ.get("SPRING_BASE_URL", "http://localhost:8080")
INTERNAL_KEY = os.environ.get("DATA_COLLECTION_INTERNAL_KEY", "")


def fetch_earnings_from_yfinance() -> List[Dict[str, Any]]:
    """yfinance로 Earnings Calendar 수집 (설치 시에만 동작)"""
    try:
        import yfinance as yf
    except ImportError:
        return []
    items = []
    try:
        today = datetime.now().date()
        end = today + timedelta(days=7)
        calendar = yf.earnings_dates(today, end)
        if calendar is None or calendar.empty:
            return items
        for idx, row in calendar.iterrows():
            title = f"Earnings: {idx}"
            if hasattr(row, "Report Date"):
                report_date = row.get("Report Date", today)
                if hasattr(report_date, "strftime"):
                    collected_at = report_date.strftime("%Y-%m-%dT%H:%M:%S")
                else:
                    collected_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            else:
                collected_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            items.append({
                "source": "YAHOO_FINANCE",
                "market": "US",
                "itemType": "BUZZ",
                "title": title[:500],
                "summary": None,
                "url": f"https://finance.yahoo.com/calendar/earnings?symbol={idx}",
                "collectedAt": collected_at,
                "symbol": str(idx) if hasattr(idx, "strip") else str(idx),
                "eventType": "earnings",
            })
    except Exception as e:
        print(f"yfinance 수집 오류: {e}", file=sys.stderr)
    return items


def post_to_spring(items: List[Dict[str, Any]]) -> bool:
    """Spring 내부 API로 수집 항목 전송"""
    if not INTERNAL_KEY:
        print("DATA_COLLECTION_INTERNAL_KEY 미설정", file=sys.stderr)
        return False
    if not items:
        return True
    url = f"{SPRING_BASE_URL.rstrip('/')}/api/v1/internal/collected-news"
    data = json.dumps({"items": items}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "X-Internal-Data-Key": INTERNAL_KEY,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 200:
                print(f"Spring API 오류: status={resp.status}", file=sys.stderr)
                return False
            body = json.loads(resp.read().decode())
            print(f"전송 완료: received={body.get('received', 0)}, saved={body.get('saved', 0)}")
            return True
    except urllib.error.HTTPError as e:
        print(f"Spring API HTTP 오류: {e.code} {e.reason}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Spring API 요청 실패: {e}", file=sys.stderr)
        return False


def main():
    items = fetch_earnings_from_yfinance()
    if not items:
        print("수집 항목 없음 (yfinance 미설치 또는 데이터 없음)")
    if post_to_spring(items):
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()
