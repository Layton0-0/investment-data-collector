# investment-data-collector

외부 시장·공시 데이터 수집 전용 서비스 (Batch / Cron).

- **역할**: US 일봉(yfinance), **DART 공시**, **SEC EDGAR 공시**, Yahoo 뉴스/이벤트 수집. Spring Backend가 HTTP로 호출하거나 이 서비스가 주기적으로 수집 후 Spring 내부 API로 전달.
- **DART/SEC 배치**: 이전에는 Spring Batch에서 수행했으나, **배치 역할은 이 Python 프로젝트로 이전**됨. `POST /dart-collect`, `POST /sec-collect` 호출 또는 `SCHEDULE_DART_SEC=1`로 기동 시 10분/15분 주기 자동 수집.

## 구조

```
├── collectors/
│   ├── us_daily_collector.py   # US 일봉 (yfinance)
│   ├── yahoo_collector.py     # Yahoo 이벤트 → Spring 내부 API
│   ├── dart_collector.py      # DART 공시 → Spring 내부 API
│   └── sec_edgar_collector.py # SEC EDGAR 공시 → Spring 내부 API
├── app.py                      # FastAPI: /us-daily, /dart-collect, /sec-collect, /health
├── Dockerfile
├── requirements.txt
└── Jenkinsfile
```

## API (FastAPI)

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | /health | 헬스체크 |
| POST | /us-daily | 기준일 US 종목 OHLCV 수집 (Spring이 호출) |
| POST | /dart-collect | DART 공시 수집 후 Spring POST /api/v1/internal/collected-news |
| POST | /sec-collect | SEC EDGAR 공시 수집 후 Spring 내부 API 전송 |

## 환경변수

| 변수 | 설명 |
|------|------|
| DART_API_KEY | Open DART API 인증키 (DART 수집 시 필수) |
| DART_BASE_URL | DART API 기본 URL (기본: https://opendart.fss.or.kr/api) |
| DART_COLLECT_DAYS | DART 수집 기간(일). 기본 3 |
| SEC_API_KEY | SEC API 키 (X-SEC-API-Key, SEC 수집 시 필수) |
| SEC_BASE_URL | SEC API 기본 URL (기본: https://data.sec.gov) |
| SEC_COLLECT_DAYS | SEC 수집 기간(일). 기본 3 |
| SEC_CIKS | 수집 대상 CIK 목록 (쉼표 구분). 미설정 시 Apple, Microsoft, Amazon |
| SPRING_BASE_URL | Spring 서버 URL (내부 API 전송용) |
| DATA_COLLECTION_INTERNAL_KEY | Spring investment.data.internal-api-key 와 동일 (내부 API 인증) |
| SCHEDULE_DART_SEC | 1 이면 기동 시 DART 10분/ SEC 15분 주기 스케줄러 활성화 |

## 로컬 실행

```bash
python -m pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8001
```

(Windows에서 `pip`가 PATH에 없으면 `python -m pip` 사용.)

DART/SEC 자동 스케줄을 쓰려면:

```bash
set SCHEDULE_DART_SEC=1
set DART_API_KEY=your_key
set SEC_API_KEY=your_sec_key
set DATA_COLLECTION_INTERNAL_KEY=your_internal_key
set SPRING_BASE_URL=http://localhost:8080
uvicorn app:app --host 0.0.0.0 --port 8001
```

## Docker

```bash
docker build -t investment-data-collector .
docker run -p 8001:8001 \
  -e DART_API_KEY=... \
  -e SEC_API_KEY=... \
  -e DATA_COLLECTION_INTERNAL_KEY=... \
  -e SPRING_BASE_URL=http://host.docker.internal:8080 \
  investment-data-collector
```

스케줄러 사용 시 `-e SCHEDULE_DART_SEC=1` 추가.

## Cron / Jenkins

Spring에서 더 이상 DART/SEC 배치를 실행하지 않으므로, 외부 cron 또는 Jenkins에서 주기적으로 호출할 수 있다.

```bash
# 10분마다 DART, 15분마다 SEC (예시)
curl -X POST http://localhost:8001/dart-collect
curl -X POST http://localhost:8001/sec-collect
```

또는 Python 서비스를 `SCHEDULE_DART_SEC=1`로 기동하면 서비스 내부에서 동일 주기로 실행된다.
