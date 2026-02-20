#!/usr/bin/env python3
"""
Google News RSS 수집기 (Speed 계층 - Reuters 대안)
금융/경제 키워드로 Google News RSS를 파싱하여 Spring 내부 API로 전달.

필요 환경변수:
  SPRING_BASE_URL: Spring 서버 URL (예: http://localhost:8080)
  DATA_COLLECTION_INTERNAL_KEY: Spring investment.data.internal-api-key 와 동일

이용약관 준수:
  - Google News RSS는 공개 서비스
  - 요청 간격 최소 2초 권장
  - User-Agent 명시
"""
import os
import sys
import json
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime
from typing import List, Dict, Any, Optional
from xml.etree import ElementTree as ET

SPRING_BASE_URL = os.environ.get("SPRING_BASE_URL", "http://localhost:8080")
INTERNAL_KEY = os.environ.get("DATA_COLLECTION_INTERNAL_KEY", "")

USER_AGENT = "InvestmentDataCollector/1.0 (Speed Layer; +https://github.com/investment)"

GOOGLE_NEWS_BASE = "https://news.google.com/rss/search"

SEARCH_QUERIES = [
    ("stock market", "US"),
    ("nasdaq", "US"),
    ("S&P 500", "US"),
    ("federal reserve", "US"),
    ("tech stocks", "US"),
    ("earnings report", "US"),
    ("semiconductor stocks", "US"),
]

REQUEST_INTERVAL_SEC = 2

SIGNAL_KEYWORDS_EN = [
    "surge", "plunge", "soar", "crash", "rally", "slump",
    "record high", "record low", "all-time high",
    "earnings beat", "earnings miss", "revenue",
    "merger", "acquisition", "M&A",
    "dividend", "buyback", "stock split",
    "IPO", "listing",
    "interest rate", "Fed", "Federal Reserve",
    "inflation", "recession",
    "AI", "artificial intelligence",
    "semiconductor", "chip", "nvidia", "apple", "tesla",
]


def _matches_signal_keyword(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.lower().strip()
    for kw in SIGNAL_KEYWORDS_EN:
        if kw.lower() in t:
            return True
    return False


def _parse_rss_date(date_str: Optional[str]) -> str:
    """RFC 822 형식 날짜를 ISO 형식으로 변환."""
    if not date_str:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _build_google_news_url(query: str) -> str:
    """Google News RSS URL 생성."""
    encoded_query = urllib.parse.quote(query)
    return f"{GOOGLE_NEWS_BASE}?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"


def _fetch_rss_feed(url: str) -> Optional[str]:
    """RSS 피드 XML 가져오기."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"RSS 피드 조회 실패: {url} - {e}", file=sys.stderr)
        return None


def _parse_rss_items(xml_content: str, query: str, market: str) -> List[Dict[str, Any]]:
    """RSS XML을 파싱하여 뉴스 항목 추출."""
    items = []
    try:
        root = ET.fromstring(xml_content)
        channel = root.find("channel")
        if channel is None:
            return items
        
        for item in channel.findall("item"):
            title_el = item.find("title")
            link_el = item.find("link")
            pub_date_el = item.find("pubDate")
            source_el = item.find("source")
            
            title = (title_el.text or "") if title_el is not None else ""
            link = (link_el.text or "") if link_el is not None else ""
            pub_date = (pub_date_el.text or "") if pub_date_el is not None else ""
            source_name = (source_el.text or "Google News") if source_el is not None else "Google News"
            
            if not title.strip():
                continue
            
            signal_relevant = _matches_signal_keyword(title)
            
            items.append({
                "source": f"GOOGLE_NEWS:{source_name}",
                "market": market,
                "itemType": "SPEED",
                "title": title.strip()[:500],
                "summary": f"Query: {query}",
                "url": link.strip(),
                "collectedAt": _parse_rss_date(pub_date),
                "symbol": None,
                "eventType": f"GOOGLE_{query.replace(' ', '_').upper()}",
                "signalRelevant": signal_relevant,
            })
    except ET.ParseError as e:
        print(f"RSS XML 파싱 오류: {e}", file=sys.stderr)
    
    return items


def fetch_google_news() -> List[Dict[str, Any]]:
    """Google News RSS 전체 수집."""
    all_items = []
    seen_urls = set()
    
    for query, market in SEARCH_QUERIES:
        url = _build_google_news_url(query)
        xml = _fetch_rss_feed(url)
        if xml:
            items = _parse_rss_items(xml, query, market)
            new_items = []
            for item in items:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    new_items.append(item)
            all_items.extend(new_items)
            print(f"Google News '{query}' 수집: {len(new_items)}건")
        time.sleep(REQUEST_INTERVAL_SEC)
    
    return all_items


def post_to_spring(items: List[Dict[str, Any]]) -> dict:
    """Spring 내부 API로 수집 항목 전송."""
    if not INTERNAL_KEY:
        raise ValueError("DATA_COLLECTION_INTERNAL_KEY 미설정")
    if not items:
        return {"received": 0, "saved": 0}
    
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
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"Spring API HTTP 오류: {e.code} {e.reason}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"Spring API 요청 실패: {e}", file=sys.stderr)
        raise


def main() -> int:
    items = fetch_google_news()
    if not items:
        print("Google News 수집 항목 없음")
        return 0
    try:
        result = post_to_spring(items)
        print(f"Google News 전송 완료: received={result.get('received', 0)}, saved={result.get('saved', 0)}")
        return 0
    except Exception as e:
        print(f"전송 실패: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
