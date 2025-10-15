
import socket, time, sys

def wait_host_port(host, port, timeout=0.5):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False

if __name__ == "__main__":
    # Support single 'host port' or 'host1:port1,host2:port2,...'
    if len(sys.argv) == 3:
        hosts = [(sys.argv[1], sys.argv[2])]
    else:
        combo = sys.argv[1]
        parts = combo.split(",")
        hosts = [tuple(p.split(":")) for p in parts]
    for _ in range(240):
        for h, p in hosts:
            if wait_host_port(h, p):
                print("ready:", h, p)
                sys.exit(0)
        time.sleep(1.0)
    print("not-ready")
    sys.exit(1)
