#!/usr/bin/env python3
"""
네이버 금융 뉴스 수집기 (Buzz 계층)
네이버 금융 '시장' 뉴스를 파싱하여 Spring 내부 API로 전달.

필요 환경변수:
  SPRING_BASE_URL: Spring 서버 URL (예: http://localhost:8080)
  DATA_COLLECTION_INTERNAL_KEY: Spring investment.data.internal-api-key 와 동일

이용약관 준수:
  - robots.txt 확인
  - 요청 간격 최소 2초
  - User-Agent 명시
  - 과도한 요청 금지
"""
import os
import sys
import json
import re
import time
import urllib.request
import urllib.error
from datetime import datetime
from typing import List, Dict, Any, Optional
from html.parser import HTMLParser

SPRING_BASE_URL = os.environ.get("SPRING_BASE_URL", "http://localhost:8080")
INTERNAL_KEY = os.environ.get("DATA_COLLECTION_INTERNAL_KEY", "")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

NAVER_NEWS_URLS = [
    ("https://finance.naver.com/news/mainnews.naver", "시장뉴스"),
    ("https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258", "증권뉴스"),
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
    "코스피", "코스닥", "나스닥", "다우", "S&P",
]


class NaverNewsHTMLParser(HTMLParser):
    """네이버 금융 뉴스 HTML 파서."""
    
    def __init__(self):
        super().__init__()
        self.items = []
        self.in_news_list = False
        self.in_news_item = False
        self.in_title = False
        self.current_item = {}
        self.current_data = ""
    
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        if tag == "ul" and "newsList" in attrs_dict.get("class", ""):
            self.in_news_list = True
        elif tag == "li" and self.in_news_list:
            self.in_news_item = True
            self.current_item = {}
        elif tag == "a" and self.in_news_item:
            href = attrs_dict.get("href", "")
            if href and "news" in href:
                self.in_title = True
                full_url = href
                if href.startswith("/"):
                    full_url = "https://finance.naver.com" + href
                elif not href.startswith("http"):
                    full_url = "https://finance.naver.com/news/" + href
                self.current_item["url"] = full_url
    
    def handle_endtag(self, tag):
        if tag == "ul" and self.in_news_list:
            self.in_news_list = False
        elif tag == "li" and self.in_news_item:
            if self.current_item.get("title"):
                self.items.append(self.current_item)
            self.in_news_item = False
            self.current_item = {}
        elif tag == "a" and self.in_title:
            title = self.current_data.strip()
            if title:
                self.current_item["title"] = title[:500]
            self.in_title = False
            self.current_data = ""
    
    def handle_data(self, data):
        if self.in_title:
            self.current_data += data


def _matches_signal_keyword(text: Optional[str]) -> bool:
    if not text:
        return False
    t = text.strip()
    for kw in SIGNAL_KEYWORDS_KR:
        if kw in t:
            return True
    return False


def _fetch_page(url: str) -> Optional[str]:
    """웹 페이지 HTML 가져오기."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("euc-kr", errors="ignore")
    except Exception as e:
        print(f"페이지 조회 실패: {url} - {e}", file=sys.stderr)
        return None


def _parse_news_simple(html: str, category: str) -> List[Dict[str, Any]]:
    """간단한 정규식 기반 파싱 (HTMLParser 대안)."""
    items = []
    
    pattern = r'<a[^>]*href=["\']([^"\']*news[^"\']*)["\'][^>]*>([^<]+)</a>'
    matches = re.findall(pattern, html, re.IGNORECASE)
    
    seen_titles = set()
    for href, title in matches:
        title = title.strip()
        if not title or len(title) < 5:
            continue
        if title in seen_titles:
            continue
        seen_titles.add(title)
        
        full_url = href
        if href.startswith("/"):
            full_url = "https://finance.naver.com" + href
        elif not href.startswith("http"):
            full_url = "https://finance.naver.com/news/" + href
        
        signal_relevant = _matches_signal_keyword(title)
        
        items.append({
            "source": "NAVER_FINANCE",
            "market": "KR",
            "itemType": "BUZZ",
            "title": title[:500],
            "summary": None,
            "url": full_url,
            "collectedAt": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "symbol": None,
            "eventType": f"NAVER_{category}",
            "signalRelevant": signal_relevant,
        })
    
    return items


def fetch_naver_news() -> List[Dict[str, Any]]:
    """네이버 금융 뉴스 전체 수집."""
    all_items = []
    seen_urls = set()
    
    for url, category in NAVER_NEWS_URLS:
        html = _fetch_page(url)
        if html:
            items = _parse_news_simple(html, category)
            new_items = []
            for item in items:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    new_items.append(item)
            all_items.extend(new_items)
            print(f"네이버 금융 {category} 수집: {len(new_items)}건")
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
    items = fetch_naver_news()
    if not items:
        print("네이버 금융 수집 항목 없음")
        return 0
    try:
        result = post_to_spring(items)
        print(f"네이버 금융 전송 완료: received={result.get('received', 0)}, saved={result.get('saved', 0)}")
        return 0
    except Exception as e:
        print(f"전송 실패: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
