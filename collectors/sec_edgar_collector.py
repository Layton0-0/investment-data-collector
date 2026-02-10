#!/usr/bin/env python3
"""
SEC EDGAR 공시 수집 (submissions/CIK{cik}.json)
수집 결과를 Spring 내부 API POST /api/v1/internal/collected-news 로 전달.

필요 환경변수:
  SEC_API_KEY: SEC API 키 (X-SEC-API-Key 헤더). 필수
  SEC_BASE_URL: 기본 https://data.sec.gov
  SEC_COLLECT_DAYS: 수집 기간(일). 기본 3
  SEC_CIKS: 수집 대상 CIK 목록 (쉼표 구분, 10자리). 기본 Apple,Microsoft,Amazon
  SPRING_BASE_URL: Spring 서버 URL
  DATA_COLLECTION_INTERNAL_KEY: Spring investment.data.internal-api-key 와 동일
"""
import os
import sys
import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

SEC_BASE_URL = os.environ.get("SEC_BASE_URL", "https://data.sec.gov").rstrip("/")
SEC_API_KEY = os.environ.get("SEC_API_KEY", "")
SEC_COLLECT_DAYS = int(os.environ.get("SEC_COLLECT_DAYS", "3"))
# Apple, Microsoft, Amazon
DEFAULT_CIKS = ["0000320193", "0000789019", "0001018724"]
SEC_CIKS_STR = os.environ.get("SEC_CIKS", "")
SEC_CIKS_LIST = [c.strip() for c in SEC_CIKS_STR.split(",") if c.strip()] if SEC_CIKS_STR else DEFAULT_CIKS

SPRING_BASE_URL = os.environ.get("SPRING_BASE_URL", "http://localhost:8080")
INTERNAL_KEY = os.environ.get("DATA_COLLECTION_INTERNAL_KEY", "")

USER_AGENT = "InvestmentChoi/1.0 (SEC EDGAR data collection)"


def _build_document_url(cik: str, accession_number: str, primary_document: Optional[str]) -> str:
    if not cik or not accession_number:
        return ""
    acc_no_dashes = accession_number.replace("-", "")
    doc = (primary_document or "").strip() or f"{acc_no_dashes}.htm"
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_dashes}/{doc}"


def _parse_filing_date(filing_date: Optional[str]) -> str:
    if not filing_date or len(filing_date) < 10:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        d = datetime.strptime(filing_date[:10], "%Y-%m-%d")
        return d.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def fetch_submissions_for_cik(
    base_url: str,
    api_key: str,
    cik: str,
    since_date: datetime,
) -> List[Dict[str, Any]]:
    """한 CIK에 대해 submissions JSON 조회 후 since_date 이후 제출 건만 반환."""
    url = f"{base_url}/submissions/CIK{cik}.json"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "X-SEC-API-Key": api_key,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print(f"SEC API 호출 실패 cik={cik}: {e}", file=sys.stderr)
        return []

    filings = data.get("filings") or {}
    recent = filings.get("recent") or {}
    accession_numbers = recent.get("accessionNumber") or []
    forms = recent.get("form") or []
    filing_dates = recent.get("filingDate") or []
    primary_docs = recent.get("primaryDocument") or []
    company_name = (data.get("name") or "").strip()
    cik_trimmed = (data.get("cik") or cik).strip()

    items = []
    for i in range(len(accession_numbers)):
        fd_str = filing_dates[i] if i < len(filing_dates) else None
        if not fd_str or len(fd_str) < 10:
            continue
        try:
            fd = datetime.strptime(fd_str[:10], "%Y-%m-%d").date()
            if fd < since_date.date():
                continue
        except ValueError:
            continue
        acc = accession_numbers[i]
        form = (forms[i] if i < len(forms) else "").strip()
        prim = primary_docs[i] if i < len(primary_docs) else None
        is_8k = form.upper() == "8-K"
        event_type = "8K" if is_8k else (form or "")[:500]
        title = f"{company_name} - {form} ({fd_str})" if company_name and form else (form or acc or "SEC Filing")
        if len(title) > 500:
            title = title[:500]
        summary = f"{company_name} / {form}".strip(" /") if (company_name or form) else None
        items.append({
            "source": "SEC_EDGAR",
            "market": "US",
            "itemType": "FACT",
            "title": title,
            "summary": summary,
            "url": _build_document_url(cik_trimmed, acc, prim),
            "collectedAt": _parse_filing_date(fd_str),
            "symbol": None,
            "eventType": event_type,
            "signalRelevant": is_8k,
        })
    return items


def fetch_sec_recent_filings(days: int = SEC_COLLECT_DAYS) -> List[Dict[str, Any]]:
    """설정된 CIK 목록으로 최근 N일 SEC 제출 건 수집."""
    if not SEC_API_KEY or not SEC_API_KEY.strip():
        return []
    since = datetime.now() - timedelta(days=days)
    all_items: List[Dict[str, Any]] = []
    for cik in SEC_CIKS_LIST:
        all_items.extend(fetch_submissions_for_cik(SEC_BASE_URL, SEC_API_KEY, cik, since))
    return all_items


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
            print(f"SEC EDGAR 전송 완료: received={body.get('received', 0)}, saved={body.get('saved', 0)}")
            return True
    except urllib.error.HTTPError as e:
        print(f"Spring API HTTP 오류: {e.code} {e.reason}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Spring API 요청 실패: {e}", file=sys.stderr)
        return False


def main() -> int:
    if not SEC_API_KEY or not SEC_API_KEY.strip():
        print("SEC_API_KEY 미설정", file=sys.stderr)
        return 1
    items = fetch_sec_recent_filings(SEC_COLLECT_DAYS)
    if not items:
        print("SEC EDGAR 수집 항목 없음")
    if post_to_spring(items):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
