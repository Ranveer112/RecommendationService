# RecommendationService

A free recommendation service for your marketplace, built for devs by a dev.

**Problem statement:** You own/manage a marketplace and want recommendations in it. However, you do not want to hire an ML team to build it, and you want a solution that gets you 80% there with 10% of the effort.

---

## What It Does (v1)

All interaction is via a headless REST API. A single persona — the **API Consumer** — drives every operation.

### Story 1: Manage Products
Add, update, and remove products (each with a name and one or more categories) so the recommendation engine operates on an up-to-date catalog.
- Supports bulk operations with per-item error reporting on partial failure.

### Story 2: Submit Ratings
Submit user-product ratings so the system can learn user preferences.
- Upserts on duplicate user-product pairs.
- Supports bulk operations with per-item error reporting on partial failure.

### Story 3: Retrain on New Data
The model retrains automatically when new data is submitted.
- Newly trained model is promoted immediately on a best-effort basis (no quality gate).
- Retraining does not block the API.

### Story 4: Recommend Similar Products
Get a ranked list of similar products given a product ID.
- Returns an empty list (not an error) on cold start.

### Story 5: Recommend Products for a User
Get a ranked list of recommended products for a user.
- Falls back to popular products for new users with no ratings.

### Story 6: Evaluate Recommendation Quality
Retrieve quality metrics (e.g., RMSE, precision@k) for the current model.
- Metrics are computed against the held-out validation set each retrain cycle.

---

## Prerequisites

- **Python 3.12+** — enforced via `requires-python` in `pyproject.toml` and the `.python-version` file.

---

## Development

### Setup

```bash
# Create and activate a virtual environment
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate

# Install all dependencies (runtime + dev)
pip install -r requirements-dev.txt
```

### Running Locally

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`. Interactive docs are served at `/docs` (Swagger UI) and `/redoc`.

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=app --cov-report=term-missing
```

### CI/CD

This project uses GitHub Actions for continuous integration:

- **Triggers**: Push to `main`, pull requests to `main`
- **Python version**: Read from `.python-version` (currently 3.12)
- **Steps**: Dependency install, lint (`ruff`), vulnerability scan (`pip-audit`), tests (`pytest`)
- **Caching**: Pip dependencies are cached for faster builds

The CI configuration is in `.github/workflows/ci.yml`.

---

## Deployment

### Option 1: Direct (Uvicorn)

Run the FastAPI app directly with Uvicorn on the target host.

```bash
# Install runtime dependencies only
pip install -r requirements.txt

# Start the server (adjust workers to match available CPU cores)
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Option 2: Docker

Build and run using Docker for a reproducible, isolated deployment.

```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
```

```bash
docker build -t recommendation-service .
docker run -p 8000:8000 recommendation-service
```

### Environment Notes

- The service uses **aiosqlite** (file-based SQLite). The database layer relies on SQLite-specific features (e.g., `PRAGMA foreign_keys`) and stores scores as strings to work around SQLite float precision, so it is not a drop-in swap to another database engine without code changes.
- **PyTorch** is included for the recommendation model. Consider using `--extra-index-url` for CPU-only wheels to reduce image size if GPU inference is not needed.

---

## Dependencies

- FastAPI
- Uvicorn
- Pydantic
- SQLAlchemy
- aiosqlite
- torch
- pytest (dev)
- pytest-asyncio (dev)
- pytest-cov (dev)
- httpx (dev)
- ruff (dev)
- pip-audit (dev)
