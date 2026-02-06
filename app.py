"""
US 일봉 수집 HTTP API (yfinance).
Spring UsMarketCollectionService에서 이 서비스를 호출해 JSON을 받아 TB_DAILY_STOCK 저장.
Docker 기동 시 /app/collector.py (collectors/us_daily_collector.py 복사본) 사용.
"""
import json
import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="US Daily Collector", version="1.0")

# 컨테이너 내: /app/collector.py / 로컬: collectors/us_daily_collector.py
COLLECTOR_SCRIPT = Path("/app/collector.py") if Path("/app/collector.py").exists() else Path(__file__).resolve().parent / "collectors" / "us_daily_collector.py"


class UsDailyRequest(BaseModel):
    bas_dt: str
    symbols: list[str]


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


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/us-daily")
def us_daily(req: UsDailyRequest):
    """기준일 US 종목 OHLCV 수집. Spring이 이 JSON 배열을 파싱해 DB 저장."""
    rows = run_collector(req.bas_dt, req.symbols)
    return rows
