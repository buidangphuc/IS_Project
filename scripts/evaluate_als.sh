#!/bin/bash
set -e

CSV_FILE="als_scalability_results.csv"

# Initialize CSV if not exists
if [ ! -f "$CSV_FILE" ]; then
    echo "Timestamp,Workers,DurationSeconds" > "$CSV_FILE"
fi

# Function to run job and grep execution time
run_job() {
    WORKERS=$1
    echo "----------------------------------------"
    echo "Testing with $WORKERS workers..."
    echo "----------------------------------------"
    
    echo "Scaling spark-worker to $WORKERS..."
    docker compose -f infra/docker-compose.yml up -d --scale spark-worker=$WORKERS --no-recreate
    
    # Wait for workers to be ready
    echo "Waiting 15s for workers to join cluster..."
    sleep 15
    
    echo "Submitting Spark Job..."
    # spark-worker has 2 cores each. 
    # With 1 worker, total cores = 2. With 2 workers, total cores = 4.
    # We request total-executor-cores = WORKERS * 2 to ensure we use all available cores
    CORES=$((WORKERS * 2))
    
    # Run spark-submit inside spark-master
    # Capture output
    start_ts=$(date +%s)
    
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --total-executor-cores $CORES \
        /app/src/datascience/components/batch.py --once > job_output_${WORKERS}.log 2>&1
        
    end_ts=$(date +%s)
    
    # Check for success
    if grep -q "Execution Time" job_output_${WORKERS}.log; then
        # Extract time value (e.g. "9.28") from "INFO:__main__:Execution Time: 9.28 seconds"
        RAW_LINE=$(grep "Execution Time" job_output_${WORKERS}.log | tail -n 1)
        TIME=$(echo "$RAW_LINE" | awk '{print $3}')
        
        echo "SUCCESS: $RAW_LINE"
        
        # Log to CSV
        NOW=$(date "+%Y-%m-%d %H:%M:%S")
        echo "$NOW,$WORKERS,$TIME" >> "$CSV_FILE"
        echo "Logged to $CSV_FILE"
    else
        echo "FAILURE: Could not find execution time. Check job_output_${WORKERS}.log"
        cat job_output_${WORKERS}.log | tail -n 10
    fi
}

echo "Starting ALS Scalability Evaluation..."

# Ensure infrastructure is running
docker compose -f infra/docker-compose.yml up -d spark-master spark-worker redis

# Run with 1 worker
run_job 1

# Run with 2 workers
run_job 2

echo "Evaluation Complete. Results saved to $CSV_FILE"
