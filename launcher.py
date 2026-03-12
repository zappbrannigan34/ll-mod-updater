import os
import socket
import threading
import time
import webbrowser

from app import app

HOST = "127.0.0.1"
PORT = int(os.environ.get("LLMM_PORT", "8765"))


def _wait_for_server(timeout_seconds: float = 20.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def _open_browser_when_ready() -> None:
    if _wait_for_server():
        webbrowser.open(f"http://{HOST}:{PORT}")


if __name__ == "__main__":
    threading.Thread(target=_open_browser_when_ready, daemon=True).start()
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
