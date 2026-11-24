"""真实并发测试：大批量暂存写入期间，现用账的核查与补登不被阻塞。

使用 Flask 内置线程服务器 + 独立 HTTP 请求，每个请求各自持有 SQLite 连接，
验证 WAL 下“写暂存账”和“读现用账”可以并发，且读取始终命中同一已提交状态。
"""

import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app  # noqa: E402
from app.reconcile import MOD  # noqa: E402

DEMO_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "demo")


def big_csv(n: int) -> bytes:
    lines = ["事件号,计数"]
    count = 1000
    for i in range(1, n + 1):
        lines.append(f"{i},{count}")
        count = (count + 37) % MOD
    return "\n".join(lines).encode()


def http_post(url: str, data: bytes | None = None,
              content_type: str = "application/json", json_body: bytes | None = None):
    body = json_body if json_body is not None else data
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def http_get(url: str):
    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.status, resp.read()


def multipart(url: str, filename: str, payload: bytes):
    boundary = "----concurrencyboundary123"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode() + payload + f"\r\n--{boundary}--\r\n".encode()
    return http_post(url, data=body,
                     content_type=f"multipart/form-data; boundary={boundary}")


class ConcurrentImportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "conc.db")
        self.app = create_app(db_path=self.db_path)
        from werkzeug.serving import make_server
        import socket

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.httpd = make_server("127.0.0.1", self.port, self.app, threaded=True)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.port}"

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def test_verify_and_anchor_during_bulk_staging(self):
        # 现用账（6 事件）+ 3 条一致锚点
        with open(os.path.join(DEMO_DIR, "events.csv"), "rb") as fh:
            demo = fh.read()
        status, _ = multipart(f"{self.base}/api/dataset", "events.csv", demo)
        self.assertEqual(status, 200)
        for ev, utc in [(1, 1_700_000_000_000),
                        (3, 1_700_000_005_464),
                        (5, 1_700_000_095_464)]:
            s, _ = http_post(
                f"{self.base}/api/anchors",
                json_body=f'{{"event_no":{ev},"utc_ms":{utc}}}'.encode(),
            )
            self.assertEqual(s, 200)

        errors: list[str] = []
        verified_during_write: list[int] = []
        stop = threading.Event()

        def reader():
            # 连续核查直到收到停止信号；每次都必须命中现用账（dataset_id=1）
            while not stop.is_set():
                try:
                    status, body = http_post(f"{self.base}/api/verify")
                    if status != 200:
                        errors.append(f"verify status {status}")
                        return
                    payload = json.loads(body)
                    if payload["dataset_id"] != 1:
                        errors.append("核查读到了非现用账版本")
                        return
                    if payload["result"]["status"] != "consistent":
                        errors.append("现用账核查结论异常")
                        return
                    verified_during_write.append(1)
                except Exception as exc:  # 网络层异常也要暴露
                    errors.append(repr(exc))
                    return
                time.sleep(0.002)

        t = threading.Thread(target=reader)
        t.start()
        time.sleep(0.05)  # 让读线程先跑起来

        # 大批量暂存（5 万事件），写的是新 dataset_id，不碰现用账
        status, body = multipart(
            f"{self.base}/api/imports/start", "big.csv", big_csv(50_000)
        )
        self.assertEqual(status, 200, body)
        import_id = json.loads(body)["import_meta"]["id"]

        # 暂存结束后再让读线程跑一小会儿，确认期间始终可读
        time.sleep(0.2)
        stop.set()
        t.join(timeout=5)
        self.assertFalse(t.is_alive(), "读线程未及时结束")
        self.assertEqual(errors, [])
        self.assertGreater(len(verified_during_write), 5,
                           "暂存写入期间应当完成多次现用账核查")

        # 暂存期间补登的锚点也落在现用账
        status, _ = http_post(
            f"{self.base}/api/anchors",
            json_body=b'{"event_no":2,"utc_ms":1700000001000}',
        )
        self.assertEqual(status, 200)
        state = json.loads(http_get(f"{self.base}/api/state")[1])
        self.assertEqual(state["dataset"]["id"], 1)
        self.assertEqual(len(state["anchors"]), 4)

        # 提交切换：O(1)，新账生效、旧锚点退出
        status, body = http_post(f"{self.base}/api/imports/{import_id}/commit")
        self.assertEqual(status, 200, body)
        state = json.loads(http_get(f"{self.base}/api/state")[1])
        self.assertEqual(state["dataset"]["event_count"], 50_000)
        self.assertEqual(state["anchors"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
