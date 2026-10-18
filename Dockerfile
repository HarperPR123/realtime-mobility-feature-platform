FROM python:3.11-slim-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    FEAST_USAGE=False \
    SPARK_LOCAL_IP=127.0.0.1
WORKDIR /app
COPY requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir -r requirements.lock && pip check
COPY src /app/src
COPY feature_repo /app/feature_repo
COPY tests /app/tests
CMD ["python", "-c", "print('mobility image ready')"]