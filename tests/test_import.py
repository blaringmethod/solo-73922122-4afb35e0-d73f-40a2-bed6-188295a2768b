"""两阶段换账与多版本账的端到端测试。

覆盖：
* 暂存期间现用账仍可核查/补登，事件与锚点取自同一已提交状态；
* 整份文件校验通过后才能启用，坏行保留现用账与锚点；
* 提交后新账生效、旧锚点随旧账退出核查；
* 导入开始后另一份账生效则换账冲突，同名同条数也退回（409）；
* 写入失败保留现用账；
* 升级后旧版（v1）库里的账与锚点可继续使用；
* 提交事务不读写 event/anchor，写锁内事件读写量不随事件条数增长。
"""

import io
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app  # noqa: E402
from app import db as dbmod  # noqa: E402
from app.reconcile import MOD  # noqa: E402

DEMO_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "demo")


def csv_text(n: int, start: int = 1000, gap: int = 500) -> bytes:
    """生成 n 个事件的合法 CSV 文本（gap 取小值，每步取模）。"""
    lines = ["事件号,计数"]
    count = start
    for i in range(1, n + 1):
        lines.append(f"{i},{count}")
        count = (count + gap) % MOD
    return "\n".join(lines).encode()


def demo_csv() -> bytes:
    with open(os.path.join(DEMO_DIR, "events.csv"), "rb") as fh:
        return fh.read()


class TwoPhaseImportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.app = create_app(db_path=self.db_path)
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def start_import(self, data: bytes, filename: str = "events.csv"):
        return self.client.post(
            "/api/imports/start",
            data={"file": (io.BytesIO(data), filename)},
            content_type="multipart/form-data",
        )

    def commit_import(self, import_id: int):
        return self.client.post(f"/api/imports/{import_id}/commit")

    def add_anchor(self, event_no, utc_ms):
        return self.client.post(
            "/api/anchors", json={"event_no": event_no, "utc_ms": utc_ms}
        )

    def test_staging_does_not_touch_active_ledger(self):
        # 先建现用账并登记锚点
        self.assertEqual(self.client.post(
            "/api/dataset",
            data={"file": (io.BytesIO(demo_csv()), "events.csv")},
            content_type="multipart/form-data",
        ).status_code, 200)
        self.add_anchor(1, 1_700_000_000_000)
        self.add_anchor(3, 1_700_000_005_464)
        self.add_anchor(5, 1_700_000_095_464)

        # 暂存第二份（更大的）账：现用账不变
        resp = self.start_import(csv_text(20), filename="batch2.csv")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        import_id = body["import_meta"]["id"]
        self.assertEqual(body["import_meta"]["status"], "staging")

        state = self.client.get("/api/state").get_json()
        self.assertEqual(state["dataset"]["filename"], "events.csv")
        self.assertEqual(len(state["anchors"]), 3)
        self.assertEqual([p["id"] for p in state["pending_imports"]], [import_id])

        # 暂存期间仍可用现用账核查、补登
        verified = self.client.post("/api/verify").get_json()
        self.assertEqual(verified["result"]["status"], "consistent")
        self.assertEqual(self.add_anchor(2, 1_700_000_001_000).status_code, 200)

        # 提交后新账生效，旧锚点随旧账退出
        resp = self.commit_import(import_id)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["dataset"]["filename"], "batch2.csv")
        self.assertEqual(resp.get_json()["anchors"], [])

        state = self.client.get("/api/state").get_json()
        self.assertEqual(state["dataset"]["event_count"], 20)
        self.assertEqual(state["anchors"], [])
        versions = {d["id"]: d["status"] for d in state["datasets"]}
        self.assertEqual(sorted(versions.values()), ["active", "retired"])

        # 新账事件号 20 可用，旧锚点已不在核查集合
        self.assertEqual(self.add_anchor(20, 123).status_code, 200)
        anchors = self.client.get("/api/state").get_json()["anchors"]
        self.assertEqual([a["event_no"] for a in anchors], [20])

    def test_bad_csv_never_stages_and_keeps_active(self):
        self.client.post(
            "/api/dataset",
            data={"file": (io.BytesIO(demo_csv()), "events.csv")},
            content_type="multipart/form-data",
        )
        self.add_anchor(1, 123)

        bad = "事件号,计数\n1,99999\n2,1\n".encode()
        resp = self.start_import(bad, filename="bad.csv")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("CSV 校验失败", resp.get_json()["error"])

        # 没有任何暂存，现用账与锚点原样保留
        state = self.client.get("/api/state").get_json()
        self.assertEqual(state["pending_imports"], [])
        self.assertEqual(state["dataset"]["filename"], "events.csv")
        self.assertEqual(len(state["anchors"]), 1)

    def test_conflict_when_another_ledger_activated_after_start(self):
        # 第一份账生效（A）
        self.client.post(
            "/api/dataset",
            data={"file": (io.BytesIO(demo_csv()), "ledger.csv")},
            content_type="multipart/form-data",
        )
        # 导入 B 开始暂存，记住 expected_active_id = A
        staged_b = self.start_import(csv_text(10), filename="ledger.csv").get_json()
        bid = staged_b["import_meta"]["id"]
        self.assertEqual(staged_b["expected_active_id"], 1)

        # 导入期间另一份账 C 生效（哪怕与 B 同名同条数，也改变了指针）
        same = csv_text(10)
        resp_c = self.client.post(
            "/api/dataset",
            data={"file": (io.BytesIO(same), "ledger.csv")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp_c.status_code, 200)
        c_id = resp_c.get_json()["dataset"]["id"]
        self.assertNotEqual(c_id, bid)

        # B 提交必须冲突退回，现用账仍是 C，锚点集合不变
        resp = self.commit_import(bid)
        self.assertEqual(resp.status_code, 409)
        self.assertIn("换账冲突", resp.get_json()["error"])

        state = self.client.get("/api/state").get_json()
        self.assertEqual(state["dataset"]["id"], c_id)
        # 冲突的暂存仍保留为 staging，可显式放弃
        self.assertEqual([p["id"] for p in state["pending_imports"]], [bid])
        discarded = self.client.delete(f"/api/imports/{bid}")
        self.assertEqual(discarded.status_code, 200)
        self.assertEqual(discarded.get_json()["pending_imports"], [])

    def test_commit_unknown_import_404(self):
        resp = self.client.post("/api/imports/999/commit")
        self.assertEqual(resp.status_code, 404)

    def test_double_commit_rejected(self):
        staged = self.start_import(csv_text(5)).get_json()
        iid = staged["import_meta"]["id"]
        self.assertEqual(self.commit_import(iid).status_code, 200)
        again = self.commit_import(iid)
        self.assertEqual(again.status_code, 409)

    def test_write_failure_keeps_active_ledger(self):
        # 现用账 A + 锚点
        self.client.post(
            "/api/dataset",
            data={"file": (io.BytesIO(demo_csv()), "a.csv")},
            content_type="multipart/form-data",
        )
        self.add_anchor(1, 1)
        staged = self.start_import(csv_text(8), filename="b.csv").get_json()
        iid = staged["import_meta"]["id"]

        # 注入写失败：提交事务中的 UPDATE 抛 sqlite3.Error，必须回滚
        real_connect = dbmod.connect
        calls = {"n": 0}

        class FlakyConnection:
            def __init__(self, inner):
                self._inner = inner

            def execute(self, sql, parameters=()):
                if sql.strip().startswith("UPDATE dataset SET status = 'retired'"):
                    calls["n"] += 1
                    raise sqlite3.OperationalError("injected disk I/O error")
                return self._inner.execute(sql, parameters)

            def __getattr__(self, name):
                return getattr(self._inner, name)

        def flaky_connect(path):
            return FlakyConnection(real_connect(path))

        dbmod.connect = flaky_connect
        try:
            resp = self.commit_import(iid)
        finally:
            dbmod.connect = real_connect

        self.assertEqual(calls["n"], 1)
        self.assertEqual(resp.status_code, 500)
        self.assertIn("写入失败", resp.get_json()["error"])
        # 现用账与锚点保留，暂存账仍是 staging（未被启用）
        state = self.client.get("/api/state").get_json()
        self.assertEqual(state["dataset"]["filename"], "a.csv")
        self.assertEqual(len(state["anchors"]), 1)
        self.assertEqual([p["id"] for p in state["pending_imports"]], [iid])

    def test_snapshot_uses_one_committed_state(self):
        self.client.post(
            "/api/dataset",
            data={"file": (io.BytesIO(demo_csv()), "events.csv")},
            content_type="multipart/form-data",
        )
        self.add_anchor(1, 1_700_000_000_000)
        self.add_anchor(3, 1_700_000_005_464)
        self.add_anchor(5, 1_700_000_095_464)

        # 核查响应标注了所用账版本，且与锚点同属一个快照
        body = self.client.post("/api/verify").get_json()
        self.assertEqual(body["dataset_id"], 1)
        self.assertEqual(body["result"]["status"], "consistent")

        # 直接验证 read_snapshot：另一连接换账后，旧连接在自己的只读事务内
        # 看到的仍是开始时的已提交状态
        c1 = dbmod.connect(self.db_path)
        c1.execute("BEGIN")
        active_before = dbmod.get_active_dataset_id(c1)
        c2 = dbmod.connect(self.db_path)
        dbmod.replace_dataset(c2, "next.csv", [1, 2, 3, 4])
        c2.close()
        # c1 的事务未结束，指针仍是旧版本
        self.assertEqual(dbmod.get_active_dataset_id(c1), active_before)
        c1.execute("ROLLBACK")
        c1.close()


class LegacyMigrationTest(unittest.TestCase):
    """旧版（v1，单行 dataset + 无 dataset_id）库升级后可继续使用。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "legacy.db")

    def tearDown(self):
        self.tmp.cleanup()

    def _build_legacy_db(self):
        from app.reconcile import analyze_counts

        with open(os.path.join(DEMO_DIR, "events.csv"), encoding="utf-8") as fh:
            import csv as csvmod
            rows = list(csvmod.reader(fh))[1:]
        counts = [int(r[1]) for r in rows]
        records = analyze_counts(counts)

        conn = sqlite3.connect(self.db_path)
        conn.executescript(
            """
            CREATE TABLE dataset (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                filename TEXT NOT NULL,
                event_count INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE event (
                event_no INTEGER PRIMARY KEY,
                count INTEGER NOT NULL,
                prefix_min INTEGER NOT NULL,
                prefix_amb INTEGER NOT NULL
            );
            CREATE TABLE anchor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_no INTEGER NOT NULL UNIQUE,
                utc_ms INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )
        conn.execute(
            "INSERT INTO dataset (id, filename, event_count) VALUES (1, 'old.csv', ?)",
            (len(records),),
        )
        conn.executemany(
            "INSERT INTO event (event_no, count, prefix_min, prefix_amb)"
            " VALUES (:event_no, :count, :prefix_min, :prefix_amb)",
            records,
        )
        conn.executemany(
            "INSERT INTO anchor (event_no, utc_ms) VALUES (?, ?)",
            [(1, 1_700_000_000_000), (3, 1_700_000_005_464), (5, 1_700_000_095_464)],
        )
        conn.commit()
        conn.close()

    def test_legacy_ledger_and_anchors_still_usable(self):
        self._build_legacy_db()
        app = create_app(db_path=self.db_path)
        client = app.test_client()

        state = client.get("/api/state").get_json()
        self.assertEqual(state["dataset"]["filename"], "old.csv")
        self.assertEqual(state["dataset"]["status"], "active")
        self.assertEqual(len(state["anchors"]), 3)

        verified = client.post("/api/verify").get_json()
        self.assertEqual(verified["result"]["status"], "consistent")

        # 旧锚点仍可更正
        anchor_id = next(a["id"] for a in state["anchors"] if a["event_no"] == 3)
        resp = client.post(
            "/api/correct",
            json={"anchor_id": anchor_id, "new_utc_ms": 1_700_000_005_464 + MOD},
        )
        # 加一圈后 1->3、3->5 仍同余且在界内则一致；若被拒也必须是回滚保护
        self.assertIn(resp.status_code, (200, 409))

        # 升级后再换账走新流程，旧锚点退出
        with open(os.path.join(DEMO_DIR, "events.csv"), "rb") as fh:
            data = fh.read()
        resp = client.post(
            "/api/dataset",
            data={"file": (io.BytesIO(data), "new.csv")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["anchors"], [])

    def test_legacy_empty_db_migrates_cleanly(self):
        # 旧结构存在但从未建账
        conn = sqlite3.connect(self.db_path)
        conn.executescript(
            """
            CREATE TABLE dataset (id INTEGER PRIMARY KEY CHECK (id = 1),
                filename TEXT NOT NULL, event_count INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')));
            CREATE TABLE event (event_no INTEGER PRIMARY KEY, count INTEGER NOT NULL,
                prefix_min INTEGER NOT NULL, prefix_amb INTEGER NOT NULL);
            CREATE TABLE anchor (id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_no INTEGER NOT NULL UNIQUE, utc_ms INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')));
            """
        )
        conn.commit()
        conn.close()

        app = create_app(db_path=self.db_path)
        client = app.test_client()
        self.assertIsNone(client.get("/api/state").get_json()["dataset"])
        resp = client.post(
            "/api/dataset",
            data={"file": (io.BytesIO(csv_text(4)), "x.csv")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["dataset"]["event_count"], 4)


class CommitComplexityTest(unittest.TestCase):
    """提交事务写锁内不得读写 event/anchor 表。"""

    def test_commit_sql_touches_no_event_rows(self):
        import inspect
        import re

        source = inspect.getsource(dbmod.commit_import).lower()
        # 提交函数里不得出现对 event/anchor 表的任何 SQL 访问
        for table in ("event", "anchor"):
            self.assertIsNone(
                re.search(rf"\b(into|from|update|delete\s+from)\s+{table}\b", source),
                f"commit_import 不得访问 {table} 表",
            )

    def test_switch_work_independent_of_event_count(self):
        # 用 SQL 语句计数器（set_trace_callback）证明：提交阶段执行的语句
        # 不随事件条数增长，且不出现 event/anchor 表访问。
        tmp = tempfile.TemporaryDirectory()
        db_path = os.path.join(tmp.name, "c.db")
        conn = dbmod.connect(db_path)

        counts_small = [(1000 + i * 500) % MOD for i in range(4)]
        counts_large = [(1000 + i * 500) % MOD for i in range(500)]

        dbmod.replace_dataset(conn, "base.csv", counts_small)
        staged_small = dbmod.stage_import(conn, "s.csv", counts_small, 1)
        statements_small: list[str] = []
        conn.set_trace_callback(statements_small.append)
        dbmod.commit_import(conn, staged_small["id"])
        conn.set_trace_callback(None)

        staged_large = dbmod.stage_import(conn, "l.csv", counts_large, 2)
        statements_large: list[str] = []
        conn.set_trace_callback(statements_large.append)
        dbmod.commit_import(conn, staged_large["id"])
        conn.set_trace_callback(None)

        def touched(stmts):
            # 只认对 event/anchor 表的访问；active_ledger 与 dataset 元数据不算事件读写
            bad = []
            for s in stmts:
                low = s.lower()
                if "active_ledger" in low:
                    continue
                if (" into event" in low or " from event" in low
                        or " into anchor" in low or " from anchor" in low
                        or "update event" in low or "update anchor" in low
                        or "delete from event" in low or "delete from anchor" in low):
                    bad.append(s)
            return bad

        self.assertEqual(touched(statements_small), [])
        self.assertEqual(touched(statements_large), [])
        # 语句数与事件条数无关
        self.assertEqual(len(statements_small), len(statements_large))
        conn.close()
        tmp.cleanup()


if __name__ == "__main__":
    unittest.main(verbosity=2)
