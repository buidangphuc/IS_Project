import os, time, datetime, subprocess, sys

def seconds_until(hhmm: str) -> float:
    now = datetime.datetime.now()
    h, m = map(int, hhmm.split(":"))
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= now:
        target += datetime.timedelta(days=1)
    return (target - now).total_seconds()

def run_offline_once():
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    print("[scheduler] running offline pipeline...")
    rc = subprocess.call([sys.executable, "-m", "datascience.pipeline.offline"], env=env)
    print("[scheduler] offline pipeline finished with code", rc)
    return rc

def main():
    run_at = os.getenv("RUN_AT", "").strip()
    interval_hours = float(os.getenv("INTERVAL_HOURS", "24"))

    # first run immediately
    run_offline_once()

    while True:
        if run_at:
            sleep_s = seconds_until(run_at)
        else:
            sleep_s = max(60.0, interval_hours * 3600.0)
        print(f"[scheduler] sleeping for ~{int(sleep_s)}s")
        time.sleep(sleep_s)
        run_offline_once()

if __name__ == "__main__":
    main()
