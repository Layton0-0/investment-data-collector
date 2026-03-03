#!/usr/bin/env python3
"""
SEC EDGAR 공시 수집 (submissions/CIK{cik}.json)
수집 결과를 Spring 내부 API POST /api/v1/internal/collected-news 로 전달.

유니버스: SEC_CIKS가 있으면 해당 CIK만 사용. 없으면 SEC_UNIVERSE(top100|top200|top500)에 따라
매 실행마다 SEC company_tickers.json을 새로 받아와 그 시점 기준 상위 N개 CIK로 수집.
(캐시 없음. TOP100/200/500은 상장·변동에 따라 매일 달라지므로 매번 최신 목록 수신 후 진행.)
"""
import os
import sys
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

SEC_BASE_URL = os.environ.get("SEC_BASE_URL", "https://data.sec.gov").rstrip("/")
SEC_API_KEY = (os.environ.get("SEC_API_KEY") or "").strip()
SEC_COLLECT_DAYS = int(os.environ.get("SEC_COLLECT_DAYS", "7"))
SEC_UNIVERSE = (os.environ.get("SEC_UNIVERSE") or "top200").strip().lower()
SEC_CIKS_STR = os.environ.get("SEC_CIKS", "").strip()
# SEC_CIKS가 명시되면 그대로 사용; 없으면 company_tickers에서 로드
SEC_CIKS_LIST: List[str] = (
    [c.strip() for c in SEC_CIKS_STR.split(",") if c.strip()]
    if SEC_CIKS_STR
    else []  # 나중에 resolve_sec_ciks()로 채움
)
SEC_RATE_LIMIT_DELAY = 0.11  # SEC 10 req/sec 준수

SPRING_BASE_URL = os.environ.get("SPRING_BASE_URL", "http://localhost:8080")
INTERNAL_KEY = os.environ.get("DATA_COLLECTION_INTERNAL_KEY", "")

# SEC는 User-Agent에 연락처(이메일) 포함을 권장. 없으면 403 가능.
SEC_USER_AGENT = (
    os.environ.get("SEC_USER_AGENT") or "InvestmentChoi/1.0 (SEC EDGAR; admin@example.com)"
).strip() or "InvestmentChoi/1.0 (SEC EDGAR; admin@example.com)"

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_UNIVERSE_TOP = {"top100": 100, "top200": 200, "top500": 500}


def _cik_to_10(cik_any: Any) -> str:
    """CIK를 10자리 문자열로 (앞 0 패딩)."""
    if cik_any is None:
        return ""
    if isinstance(cik_any, int):
        return str(cik_any).zfill(10)
    s = str(cik_any).strip()
    if not s.isdigit():
        return ""
    return s.zfill(10)


def fetch_company_tickers_ciks(
    _base_url: str, user_agent: str, limit: int = 200
) -> List[str]:
    """
    매 호출 시 SEC에서 최신 company_tickers.json을 받아와 CIK 목록 반환. 캐시 없음.
    (TOP N은 상장·변동으로 매일 달라지므로 매 실행마다 수신 후 진행.)
    limit만큼 상위(인덱스 순) 사용. 구조: {"0": {"cik_str": 320193, "ticker": "AAPL", ...}, ...}
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    req = urllib.request.Request(url, headers={"User-Agent": user_agent}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        print(f"SEC company_tickers 최신 수신 완료, 상위 {limit}개 CIK 사용", file=sys.stderr)
    except Exception as e:
        print(f"SEC company_tickers 로드 실패: {e}", file=sys.stderr)
        return []
    if not isinstance(data, dict):
        return []
    ciks: List[str] = []
    keys = sorted(data.keys(), key=lambda k: int(k) if k.isdigit() else 0)
    for k in keys:
        if len(ciks) >= limit:
            break
        ent = data.get(k)
        if not isinstance(ent, dict):
            continue
        cik = _cik_to_10(ent.get("cik_str") or ent.get("cik"))
        if cik and cik not in ciks:
            ciks.append(cik)
    return ciks


def resolve_sec_ciks() -> List[str]:
    """
    수집에 사용할 CIK 목록. SEC_CIKS가 있으면 해당 고정 목록.
    없으면 매번 SEC에서 최신 company_tickers 수신 후 SEC_UNIVERSE(top100|top200|top500)만큼 사용.
    """
    if SEC_CIKS_STR:
        return [c.strip() for c in SEC_CIKS_STR.split(",") if c.strip()]
    n = _UNIVERSE_TOP.get(SEC_UNIVERSE, 200)
    ciks = fetch_company_tickers_ciks(SEC_BASE_URL, SEC_USER_AGENT, limit=n)
    if not ciks:
        # fallback: 소수 대형주 (레거시 호환)
        return ["0000320193", "0000789019", "0001018724", "0001640148", "0001652044"]
    return ciks


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
    user_agent: str,
) -> List[Dict[str, Any]]:
    """한 CIK에 대해 submissions JSON 조회 후 since_date 이후 제출 건만 반환."""
    url = f"{base_url}/submissions/CIK{cik}.json"
    headers = {"User-Agent": user_agent}
    if api_key:
        headers["X-SEC-API-Key"] = api_key
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"SEC API HTTP 오류 cik={cik}: {e.code} {e.reason}", file=sys.stderr)
        if e.code == 403:
            print("  → User-Agent에 연락처 이메일 포함 권장. SEC_USER_AGENT 환경변수 설정.", file=sys.stderr)
        return []
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


def fetch_sec_recent_filings(days: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    최근 N일 SEC 제출 건 수집. 매 실행 시 최신 유니버스(company_tickers) 수신 후 진행.
    SEC 10 req/sec 준수.
    """
    n_days = days if days is not None else SEC_COLLECT_DAYS
    n_days = max(7, n_days)
    since = datetime.now() - timedelta(days=n_days)
    ciks = resolve_sec_ciks()  # SEC_CIKS 없으면 매번 최신 company_tickers에서 로드
    if not ciks:
        return []
    all_items: List[Dict[str, Any]] = []
    for i, cik in enumerate(ciks):
        if i > 0:
            time.sleep(SEC_RATE_LIMIT_DELAY)
        all_items.extend(
            fetch_submissions_for_cik(
                SEC_BASE_URL, SEC_API_KEY, cik, since, SEC_USER_AGENT
            )
        )
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
    items = fetch_sec_recent_filings(SEC_COLLECT_DAYS)
    if not items:
        print("SEC EDGAR 수집 항목 없음")
    if post_to_spring(items):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
