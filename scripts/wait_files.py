import sys, time
from pathlib import Path

def wait_files(paths, retries=300, sleep=1.0):
    for _ in range(retries):
        if all(Path(p).exists() for p in paths):
            return True
        time.sleep(sleep)
    return False

if __name__ == "__main__":
    ok = wait_files(sys.argv[1:])
    print("ready" if ok else "not-ready")
    sys.exit(0 if ok else 1)
