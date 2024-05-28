import signal

import uvicorn

from weasel.main import handle_exit


def main():
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)
    uvicorn.run(
        "weasel.main:app",
        host="127.0.0.1",
        port=8767,
        # reload=True,
        # reload_dirs=[Path("./src").resolve()],
    )


if __name__ == "__main__":
    main()
