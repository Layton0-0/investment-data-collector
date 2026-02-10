"""
데이터 수집 HTTP API (US 일봉, DART 공시, SEC EDGAR 공시).
- US 일봉: Spring UsMarketCollectionService가 호출해 TB_DAILY_STOCK 저장.
- DART/SEC 공시: 이 서비스가 수집 후 Spring POST /api/v1/internal/collected-news 로 전달.
Docker 기동 시 /app/collector.py (collectors/us_daily_collector.py 복사본) 사용.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Investment Data Collector", version="1.0")

# 컨테이너 내: /app/collector.py / 로컬: collectors/us_daily_collector.py
COLLECTOR_SCRIPT = Path("/app/collector.py") if Path("/app/collector.py").exists() else Path(__file__).resolve().parent / "collectors" / "us_daily_collector.py"

# DART/SEC 스케줄 사용 여부 (1이면 기동 시 10분/15분 주기로 수집)
SCHEDULE_DART_SEC = os.environ.get("SCHEDULE_DART_SEC", "").strip() == "1"


def _post_collected_news(items: list[dict]) -> dict:
    """Spring 내부 API로 수집 항목 전송. 응답 { received, saved } 반환."""
    internal_key = os.environ.get("DATA_COLLECTION_INTERNAL_KEY", "")
    spring_url = os.environ.get("SPRING_BASE_URL", "http://localhost:8080").rstrip("/")
    if not internal_key:
        raise HTTPException(status_code=503, detail="DATA_COLLECTION_INTERNAL_KEY not set")
    if not items:
        return {"received": 0, "saved": 0}
    import urllib.request
    url = f"{spring_url}/api/v1/internal/collected-news"
    data = json.dumps({"items": items}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "X-Internal-Data-Key": internal_key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Spring API error: {e}")


def run_collector(bas_dt: str, symbols: list[str]) -> list[dict]:
    if not COLLECTOR_SCRIPT.exists():
        raise RuntimeError("collector script not found")
    symbols_str = ",".join(s for s in symbols if s and str(s).strip())
    if not symbols_str:
        return []
    try:
        proc = subprocess.run(
            [sys.executable, str(COLLECTOR_SCRIPT), "--bas-dt", bas_dt, "--symbols", symbols_str],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(COLLECTOR_SCRIPT.parent),
        )
        if proc.returncode != 0:
            return []
        out = (proc.stdout or "").strip()
        if not out:
            return []
        return json.loads(out)
    except (json.JSONDecodeError, subprocess.TimeoutExpired, FileNotFoundError) as e:
        raise HTTPException(status_code=500, detail=str(e))


class UsDailyRequest(BaseModel):
    bas_dt: str
    symbols: list[str]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/us-daily")
def us_daily(req: UsDailyRequest):
    """기준일 US 종목 OHLCV 수집. Spring이 이 JSON 배열을 파싱해 DB 저장."""
    rows = run_collector(req.bas_dt, req.symbols)
    return rows


@app.post("/dart-collect")
def dart_collect():
    """DART 공시 수집 후 Spring 내부 API로 전송. (배치 역할: Python에서 수행)"""
    from collectors.dart_collector import fetch_dart_for_days, to_collected_items
    raw = fetch_dart_for_days()
    items = to_collected_items(raw)
    result = _post_collected_news(items)
    return result


@app.post("/sec-collect")
def sec_collect():
    """SEC EDGAR 공시 수집 후 Spring 내부 API로 전송. (배치 역할: Python에서 수행)"""
    from collectors.sec_edgar_collector import fetch_sec_recent_filings
    items = fetch_sec_recent_filings()
    result = _post_collected_news(items)
    return result


# ----- 스케줄러 (SCHEDULE_DART_SEC=1 일 때만) -----
_scheduler = None


def _run_dart_job():
    try:
        from collectors.dart_collector import fetch_dart_for_days, to_collected_items
        raw = fetch_dart_for_days()
        items = to_collected_items(raw)
        if items:
            _post_collected_news(items)
    except Exception as e:
        print(f"DART 스케줄 실행 오류: {e}", file=sys.stderr)


def _run_sec_job():
    try:
        from collectors.sec_edgar_collector import fetch_sec_recent_filings
        items = fetch_sec_recent_filings()
        if items:
            _post_collected_news(items)
    except Exception as e:
        print(f"SEC 스케줄 실행 오류: {e}", file=sys.stderr)


@app.on_event("startup")
def startup():
    if not SCHEDULE_DART_SEC:
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.interval import IntervalTrigger
        global _scheduler
        _scheduler = BackgroundScheduler(timezone="Asia/Seoul")
        _scheduler.add_job(_run_dart_job, IntervalTrigger(minutes=10), id="dart")
        _scheduler.add_job(_run_sec_job, IntervalTrigger(minutes=15), id="sec")
        _scheduler.start()
        print("DART/SEC 스케줄러 시작: DART 10분, SEC 15분 주기", file=sys.stderr)
    except ImportError:
        print("SCHEDULE_DART_SEC=1 이지만 apscheduler 미설치. pip install apscheduler", file=sys.stderr)


@app.on_event("shutdown")
def shutdown():
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
