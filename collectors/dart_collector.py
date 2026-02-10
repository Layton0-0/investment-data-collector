#!/usr/bin/env python3
"""
Open DART 공시 목록 수집 (list.json API)
수집 결과를 Spring 내부 API POST /api/v1/internal/collected-news 로 전달.

필요 환경변수:
  DART_API_KEY: Open DART API 인증키 (필수)
  DART_BASE_URL: 기본 https://opendart.fss.or.kr/api
  DART_COLLECT_DAYS: 수집 기간(일). 기본 3
  SPRING_BASE_URL: Spring 서버 URL (예: http://localhost:8080)
  DATA_COLLECTION_INTERNAL_KEY: Spring investment.data.internal-api-key 와 동일
"""
import os
import sys
import json
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

DART_BASE_URL = os.environ.get("DART_BASE_URL", "https://opendart.fss.or.kr/api").rstrip("/")
DART_API_KEY = os.environ.get("DART_API_KEY", "")
DART_COLLECT_DAYS = int(os.environ.get("DART_COLLECT_DAYS", "3"))
SPRING_BASE_URL = os.environ.get("SPRING_BASE_URL", "http://localhost:8080")
INTERNAL_KEY = os.environ.get("DATA_COLLECTION_INTERNAL_KEY", "")

MAX_PAGE_COUNT = 100

# 시그널 반영용 키워드 (13-news-collection-design: 무상증자·영업익 30% 증가 등)
DART_SIGNAL_KEYWORDS = [
    "무상증자",
    "유상증자",
    "영업익",
    "30% 증가",
    "단일판매공급계약",
    "실적",
    "배당",
    "자기주식",
    "M&A",
    "인수",
]


def _matches_signal_keyword(text: Optional[str]) -> bool:
    """제목/보고서명에 시그널 키워드가 포함되면 True."""
    if not text or not isinstance(text, str):
        return False
    t = text.strip()
    if not t:
        return False
    for kw in DART_SIGNAL_KEYWORDS:
        if kw in t:
            return True
    return False


def _build_viewer_url(rcept_no: Optional[str]) -> str:
    if not rcept_no or not str(rcept_no).strip():
        return ""
    return f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"


def _parse_rcept_dt(rcept_dt: Optional[str]) -> str:
    if not rcept_dt or len(rcept_dt) < 8:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        d = datetime.strptime(rcept_dt[:8], "%Y%m%d")
        return d.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def fetch_dart_list(bgn_de: str, end_de: str, page_no: int) -> List[Dict[str, Any]]:
    """DART list.json 한 페이지 조회. 성공 시 list 항목 반환."""
    if not DART_API_KEY or not DART_API_KEY.strip():
        return []
    url = (
        f"{DART_BASE_URL}/list.json"
        f"?crtfc_key={urllib.parse.quote(DART_API_KEY)}"
        f"&bgn_de={bgn_de}&end_de={end_de}"
        f"&page_no={page_no}&page_count={MAX_PAGE_COUNT}"
    )
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            if data.get("status") != "000":
                return []
            lst = data.get("list")
            return lst if isinstance(lst, list) else []
    except Exception as e:
        print(f"DART API 호출 실패: {e}", file=sys.stderr)
        return []


def fetch_dart_for_days(days: int = DART_COLLECT_DAYS) -> List[Dict[str, Any]]:
    """최근 N일 공시 전체 조회 (페이지네이션)."""
    end_d = datetime.now().date()
    bgn_d = end_d - timedelta(days=days)
    bgn_de = bgn_d.strftime("%Y%m%d")
    end_de = end_d.strftime("%Y%m%d")
    all_items: List[Dict[str, Any]] = []
    page_no = 1
    while True:
        page = fetch_dart_list(bgn_de, end_de, page_no)
        if not page:
            break
        all_items.extend(page)
        if len(page) < MAX_PAGE_COUNT:
            break
        page_no += 1
    return all_items


def to_collected_items(raw_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """DART list 항목을 Spring collected-news items 형식으로 변환.
    시그널 키워드 매칭 시 eventType에 DART_SIGNAL: 접두사로 저장해 시그널 반영 대상 표시.
    """
    items = []
    for row in raw_list:
        rcept_no = row.get("rcept_no") or ""
        report_nm = (row.get("report_nm") or "")[:500]
        corp_name = row.get("corp_name") or ""
        flr_nm = row.get("flr_nm") or ""
        summary = f"{corp_name} / {flr_nm}".strip(" /") if (corp_name or flr_nm) else None
        stock_code = row.get("stock_code")
        symbol = str(stock_code).strip() if stock_code else None
        signal_relevant = _matches_signal_keyword(report_nm)
        if signal_relevant:
            event_type = "DART_SIGNAL:" + (report_nm[:490] if report_nm else "")
        else:
            event_type = (report_nm or "")[:500]
        items.append({
            "source": "DART",
            "market": "KR",
            "itemType": "FACT",
            "title": report_nm,
            "summary": summary,
            "url": _build_viewer_url(rcept_no),
            "collectedAt": _parse_rcept_dt(row.get("rcept_dt")),
            "symbol": symbol,
            "eventType": event_type,
            "signalRelevant": signal_relevant,
        })
    return items


def post_to_spring(items: List[Dict[str, Any]]) -> bool:
    """Spring 내부 API로 수집 항목 전송."""
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
        with urllib.request.urlopen(req, timeout=60) as resp:
            if resp.status != 200:
                print(f"Spring API 오류: status={resp.status}", file=sys.stderr)
                return False
            body = json.loads(resp.read().decode())
            print(f"DART 전송 완료: received={body.get('received', 0)}, saved={body.get('saved', 0)}")
            return True
    except urllib.error.HTTPError as e:
        print(f"Spring API HTTP 오류: {e.code} {e.reason}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Spring API 요청 실패: {e}", file=sys.stderr)
        return False


def main() -> int:
    if not DART_API_KEY or not DART_API_KEY.strip():
        print("DART_API_KEY 미설정", file=sys.stderr)
        return 1
    raw = fetch_dart_for_days(DART_COLLECT_DAYS)
    items = to_collected_items(raw)
    if not items:
        print("DART 수집 항목 없음")
    if post_to_spring(items):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
