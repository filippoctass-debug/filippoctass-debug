FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN addgroup --gid 1000 appuser \
 && adduser --uid 1000 --gid 1000 --disabled-password --gecos "" appuser

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY . /app

RUN mkdir -p /app/data /app/certs \
 && chown -R appuser:appuser /app

USER appuser
CMD ["python","/app/edge_driver.py"]
