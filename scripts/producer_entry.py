import subprocess, sys, os
from datascience.config.loader import load_all

def main():
    cfg, _, P = load_all()
    # wait for kafka
    rc = subprocess.call([sys.executable, "scripts/wait_net.py", cfg["kafka"]["bootstrap"]])
    if rc != 0:
        print("Kafka not ready"); sys.exit(1)
    # wait for parquet from offline pipeline
    rc = subprocess.call([sys.executable, "scripts/wait_files.py", f"{P['LAKE']}/ratings.parquet"])
    if rc != 0:
        print("ratings.parquet not found"); sys.exit(1)
    # run producer
    from scripts.kafka_producer_sim import main as produce_main
    produce_main()

if __name__ == "__main__":
    main()
