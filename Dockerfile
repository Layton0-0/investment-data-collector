# US 일봉 수집 서비스. Spring이 HTTP로 호출.
FROM python:3.11-slim
ENV TZ=Asia/Seoul

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collectors/ /app/collectors/
# US 일봉용 레거시 경로 (Spring이 기대하는 스크립트)
COPY collectors/us_daily_collector.py /app/collector.py
COPY app.py .

EXPOSE 8001
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]
