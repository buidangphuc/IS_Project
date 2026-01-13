FROM python:3.10-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# python deps
COPY requirements-app.txt ./requirements.txt
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
RUN uv pip install --system --no-cache -r requirements.txt

# app code
COPY . ./
ENV PYTHONPATH=/app:/app/src

CMD ["bash", "-lc", "uvicorn app:app --host 0.0.0.0 --port 8000"]
