FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# system deps + java for pyspark
RUN apt-get update && apt-get install -y \
    build-essential \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# set java environment
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# app code
COPY . ./
ENV PYTHONPATH=/app/src

CMD ["bash", "-lc", "uvicorn app:app --host 0.0.0.0 --port 8000"]
