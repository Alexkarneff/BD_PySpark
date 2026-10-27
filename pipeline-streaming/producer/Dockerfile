FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Unbuffered output so logs show up immediately in `docker compose logs`
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "producer.py"]
