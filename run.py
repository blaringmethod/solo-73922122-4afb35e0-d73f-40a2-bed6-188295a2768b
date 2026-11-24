"""本地开发入口：python run.py [--host 0.0.0.0] [--port 8000]。"""

import argparse

from app import create_app

app = create_app()


def main() -> None:
    parser = argparse.ArgumentParser(description="桥梁振动记录仪时刻核查页")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
