# CLAUDE.md

## Overview

This repository is a Python-based OMR (Optical Mark Recognition) microservice.
It replaces the previous Streamlit demo and the abandoned Go service direction.

The service exposes:
- **FastAPI HTTP** endpoints (uvicorn on `:8080`) for template parsing, recognition, verification, re-verification, and async task management.
- **Redis Stream** consumer/producer for batch image recognition jobs.
- **Nacos** for both service registration/discovery and configuration management.

## Commands

```bash
# Setup
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt

# Run service (main.py boots uvicorn under the hood)
python -m omr_service.main
# Or run uvicorn directly:
# uvicorn omr_service.main:app --host 0.0.0.0 --port 8080

# Regenerate API schemas is not required — FastAPI derives them from Pydantic models
# under omr_service/api/schemas/ at startup.

# Run tests (pytest; tests/ directory per pytest.ini testpaths)
python -m pytest -v

# Docker
docker compose build
docker compose up -d
```

## Configuration

Configuration priority: **Nacos > environment variables > defaults**.

Create a Nacos config:
- dataId: `omr-service.yaml`
- group: `DEFAULT_GROUP`

Example:

```yaml
nacos_server: 127.0.0.1:8848
nacos_namespace: public
redis:
  host: 47.99.83.217
  port: 6379
  password: your_password
  db: 1
omr_worker_count: 4
```

The loader in `config.py` flattens nested YAML keys (e.g. `redis.host`).

## Architecture

### FastAPI App (`omr_service/api/app.py`)

`create_app()` builds a FastAPI instance with:
- `/v1/recognize`, `/v1/templates/parse`, `/v1/reverify_paper`, `/v1/verify_recognition_rate`
- `/v1/tasks`, `/v1/tasks/{task_id}`
- `/v1/health`, `/v1/health/ready`
- `/v1/omr_crops/{file_path:path}`
- `/v1/docs` (Swagger UI), `/v1/openapi.json`

Routers live under `omr_service/api/routers/`, Pydantic schemas under `omr_service/api/schemas/`.

### Core Service (`omr_service/core/service.py`)

`OmrService` wraps the engine and is invoked synchronously from the FastAPI handlers
and asynchronously from the Redis Stream consumer.

### Redis Batch Flow (`omr_service/mq/`)

- `consumer.py` reads from Redis Stream `omr:batch:job` using a consumer group.
- `job_handler.py` processes jobs concurrently and writes results to `omr:batch:result`.

### Nacos (`omr_service/nacos_config.py`, `omr_service/nacos_reg.py`)

- Config client pulls config at startup and optionally listens for changes.
- Registrator registers the HTTP instance under the app-level service name `omr-service`
  with metadata containing the local debug Tag.

## Key Implementation Details

### Coordinate Scaling

Template coordinates are based on reference image dimensions and scaled at runtime.

### Golden Template Grid Generation

`StandardTemplate._generate_grid()` supports `option_axis` and `reverse_q`.

### Windows Chinese Path Handling

`cv2.imwrite()` has UTF-8 issues on Windows. The engine uses `cv2.imencode + open(filepath, 'wb')`.

## Adding New HTTP Endpoints

1. Define a Pydantic request/response model under `omr_service/api/schemas/`.
2. Add a handler function to the appropriate router in `omr_service/api/routers/`.
3. If the router does not exist yet, create it and include it in `omr_service/api/app.py`.
4. Add a test under `tests/` (e.g. `tests/api/`) using FastAPI's `TestClient`.