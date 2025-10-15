import subprocess, sys
from datascience.config.loader import load_all

def main():
    cfg, _, _ = load_all()
    rc = subprocess.call([sys.executable, "scripts/wait_net.py", cfg["kafka"]["bootstrap"].split(":")[0], cfg["kafka"]["bootstrap"].split(":")[1]])
    if rc != 0:
        print("Kafka not ready"); sys.exit(1)
    # run consumer
    from datascience.pipeline.streaming import main as consume_main
    consume_main()

if __name__ == "__main__":
    main()
