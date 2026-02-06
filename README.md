# investment-data-collector

외부 시장 데이터 수집 전용 서비스 (Batch / Cron).

- **역할**: US 일봉(yfinance), Yahoo 뉴스/이벤트 수집. Spring Backend가 HTTP로 호출하거나 Cron으로 실행.
- **항상 기동 불필요**: docker-compose / cron / Jenkins job으로 실행 가능.

## 구조

```
├── collectors/
│   ├── us_daily_collector.py
│   └── yahoo_collector.py
├── app.py              # FastAPI: POST /us-daily, GET /health
├── Dockerfile
├── requirements.txt
└── Jenkinsfile
```

## 로컬 실행

```bash
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8001
```

## Docker

```bash
docker build -t investment-data-collector .
docker run -p 8001:8001 investment-data-collector
```
