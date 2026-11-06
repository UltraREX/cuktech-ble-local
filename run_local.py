"""Run the local Web UI and BLE API as separate processes."""

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
PID_FILE = DATA / "run_local.pid"
CONSOLE_LOG = DATA / "console.log"
HOST = "127.0.0.1"
WEB_PORT = 8199
BACKEND_PORT = 18199
BACKEND_RESTART = 75


def backend_environment():
    DATA.mkdir(exist_ok=True)
    config = DATA / "config.yaml"
    if not config.exists():
        template = (ROOT / "config.example.yaml").read_bytes()
        try:
            with config.open("xb") as target:
                target.write(template)
        except FileExistsError:
            pass

    return dict(
        os.environ,
        CUKTECH_CONFIG_PATH=str(config),
        CUKTECH_HISTORY_DB_PATH=str(DATA / "port_history.db"),
        CUKTECH_LOG_FILE=str(DATA / "server.log"),
        CUKTECH_SERVER_HOST=HOST,
        CUKTECH_SERVER_PORT=str(BACKEND_PORT),
        PYTHONPATH=str(ROOT / "backend" / "src"),
        PYTHONUTF8="1",
        PYTHONUNBUFFERED="1",
        PYTHONDONTWRITEBYTECODE="1",
    )


def console_python(path):
    # pythonw has no usable stdio; the backend's log setup calls sys.stderr.isatty().
    path = Path(path)
    return str(path.with_name("python.exe")) if path.name.lower() == "pythonw.exe" else str(path)


def write_pid_file():
    PID_FILE.write_text(str(os.getpid()), encoding="ascii")


def remove_pid_file():
    try:
        if PID_FILE.read_text(encoding="ascii").strip() == str(os.getpid()):
            PID_FILE.unlink()
    except OSError:
        pass


def run(web_port, background=False):
    env = backend_environment()
    python = console_python(sys.executable)
    if os.name == "nt" and sys.prefix != sys.base_prefix:
        # Bypass the venv redirector so the child handles belong to the servers.
        # Preserve venv discovery as in multiprocessing's Windows launcher.
        env["__PYVENV_LAUNCHER__"] = python
        python = console_python(sys._base_executable)
    backend_command = [python, str(ROOT / "backend" / "ha_server.py")]
    gateway_command = [
        python, str(ROOT / "web_gateway.py"),
        "--host", HOST, "--port", str(web_port),
        "--backend", f"http://{HOST}:{BACKEND_PORT}",
    ]
    popen_args = dict(cwd=ROOT, env=env)
    if background:
        log = open(CONSOLE_LOG, "a", encoding="utf-8", buffering=1)
        sys.stdout = sys.stderr = log
        popen_args.update(
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        print(f"--- {time.strftime('%Y-%m-%d %H:%M:%S')} started in background (pid {os.getpid()}) ---")

    write_pid_file()
    backend = gateway = None
    try:
        backend = subprocess.Popen(backend_command, **popen_args)
        gateway = subprocess.Popen(gateway_command, **popen_args)
        print(f"Web UI: http://{HOST}:{web_port}/", flush=True)
        print(f"Setup:  http://{HOST}:{web_port}/config.html", flush=True)
        if not background:
            print("Press Ctrl+C to stop both services.", flush=True)
        while True:
            gateway_status = gateway.poll()
            if gateway_status is not None:
                print(f"Web UI process exited ({gateway_status}).", file=sys.stderr)
                return gateway_status or 1

            backend_status = backend.poll()
            if backend_status == BACKEND_RESTART:
                print("Restarting BLE backend...", flush=True)
                backend = subprocess.Popen(backend_command, **popen_args)
            elif backend_status == 0:
                print("BLE backend stopped.", flush=True)
                return 0
            elif backend_status is not None:
                print(f"BLE backend process exited ({backend_status}).", file=sys.stderr)
                return backend_status or 1
            time.sleep(0.25)
    except KeyboardInterrupt:
        # The two console children also receive Ctrl+C. Give them time to clean up.
        deadline = time.monotonic() + 5
        for child in (gateway, backend):
            if child is not None:
                try:
                    child.wait(timeout=max(0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass
        return 0
    finally:
        for child in (gateway, backend):
            if child is not None and child.poll() is None:
                child.terminate()
                child.wait()
        remove_pid_file()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--web-port", type=int, default=WEB_PORT)
    parser.add_argument("--background", action="store_true",
                        help="no console: child output goes to data/console.log")
    args = parser.parse_args()
    if not 1 <= args.web_port <= 65535 or args.web_port == BACKEND_PORT:
        parser.error(f"--web-port must be 1–65535 and differ from {BACKEND_PORT}")
    return run(args.web_port, background=args.background)


if __name__ == "__main__":
    raise SystemExit(main())
