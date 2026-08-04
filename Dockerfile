FROM python:3.12-slim-bookworm
RUN sed -i 's|deb.debian.org|mirrors.ustc.edu.cn|g' /etc/apt/sources.list.d/*.list || true

RUN apt-get update && apt-get install -y --no-install-recommends \
    libzbar0 libjpeg-dev zlib1g-dev libgl1 libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

COPY omr_service/ ./omr_service/
COPY .env.example .env.example

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:8080/v1/health || exit 1

ENTRYPOINT ["uvicorn", "omr_service.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
