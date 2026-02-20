#!/usr/bin/env python3
"""
연합뉴스 RSS 수집기 (Speed 계층)
경제/금융 뉴스 RSS를 파싱하여 Spring 내부 API로 전달.

필요 환경변수:
  SPRING_BASE_URL: Spring 서버 URL (예: http://localhost:8080)
  DATA_COLLECTION_INTERNAL_KEY: Spring investment.data.internal-api-key 와 동일

이용약관 준수:
  - 연합뉴스 RSS는 공개 서비스
  - 요청 간격 최소 3분 권장
  - User-Agent 명시
"""
import os
import sys
import json
import time
import urllib.request
import urllib.error
from datetime import datetime
from typing import List, Dict, Any, Optional
from xml.etree import ElementTree as ET

SPRING_BASE_URL = os.environ.get("SPRING_BASE_URL", "http://localhost:8080")
INTERNAL_KEY = os.environ.get("DATA_COLLECTION_INTERNAL_KEY", "")

USER_AGENT = "InvestmentDataCollector/1.0 (Speed Layer; +https://github.com/investment)"

YONHAP_FEED_URLS = [
    ("https://www.yna.co.kr/rss/economy.xml", "경제"),
    ("https://www.yna.co.kr/rss/industry.xml", "산업"),
]

REQUEST_INTERVAL_SEC = 2

SIGNAL_KEYWORDS_KR = [
    "급등", "급락", "폭등", "폭락",
    "상한가", "하한가",
    "실적", "영업이익", "순이익", "매출",
    "인수", "합병", "M&A",
    "배당", "무상증자", "유상증자",
    "공모", "IPO", "상장",
    "금리", "기준금리",
    "환율", "달러",
    "반도체", "AI", "인공지능",
    "삼성전자", "SK하이닉스", "네이버", "카카오",
]


def _matches_signal_keyword(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.strip()
    for kw in SIGNAL_KEYWORDS_KR:
        if kw in t:
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


def _parse_rss_items(xml_content: str, category: str) -> List[Dict[str, Any]]:
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
            desc_el = item.find("description")
            pub_date_el = item.find("pubDate")
            
            title = (title_el.text or "") if title_el is not None else ""
            link = (link_el.text or "") if link_el is not None else ""
            desc = (desc_el.text or "") if desc_el is not None else ""
            pub_date = (pub_date_el.text or "") if pub_date_el is not None else ""
            
            if not title.strip():
                continue
            
            signal_relevant = _matches_signal_keyword(title) or _matches_signal_keyword(desc)
            
            items.append({
                "source": "YONHAP",
                "market": "KR",
                "itemType": "SPEED",
                "title": title.strip()[:500],
                "summary": desc.strip()[:1000] if desc else None,
                "url": link.strip(),
                "collectedAt": _parse_rss_date(pub_date),
                "symbol": None,
                "eventType": f"YONHAP_{category}",
                "signalRelevant": signal_relevant,
            })
    except ET.ParseError as e:
        print(f"RSS XML 파싱 오류: {e}", file=sys.stderr)
    
    return items


def fetch_yonhap_news() -> List[Dict[str, Any]]:
    """연합뉴스 RSS 전체 수집."""
    all_items = []
    for url, category in YONHAP_FEED_URLS:
        xml = _fetch_rss_feed(url)
        if xml:
            items = _parse_rss_items(xml, category)
            all_items.extend(items)
            print(f"연합뉴스 {category} 수집: {len(items)}건")
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
    items = fetch_yonhap_news()
    if not items:
        print("연합뉴스 수집 항목 없음")
        return 0
    try:
        result = post_to_spring(items)
        print(f"연합뉴스 전송 완료: received={result.get('received', 0)}, saved={result.get('saved', 0)}")
        return 0
    except Exception as e:
        print(f"전송 실패: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
