"""Flask 核查页应用工厂与路由。"""

from __future__ import annotations

import os
import sqlite3

from flask import Flask, jsonify, render_template, request

from . import db as dbmod
from .csv_loader import CsvError, parse_events_csv
from .reconcile import MIN_ANCHORS, verify


class RecheckConflict(RuntimeError):
    """更正后按全量锚点复核仍不一致（业务冲突，非系统故障）。"""


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path or os.environ.get(
        "DB_PATH", os.path.join(os.getcwd(), "data", "inspection.db")
    )

    def get_db() -> sqlite3.Connection:
        return dbmod.connect(app.config["DB_PATH"])

    def ok(**payload):
        return jsonify({"ok": True, **payload})

    def fail(message: str, status: int = 400, **extra):
        return jsonify({"ok": False, "error": message, **extra}), status

    def read_upload():
        """读取并解析上传的 CSV；成功返回 (filename, events)，失败返回 (None, 错误响应)。"""
        upload = request.files.get("file")
        if upload is None or upload.filename == "":
            return None, fail("请选择 CSV 文件")
        try:
            text = upload.read().decode("utf-8-sig")
            parsed = parse_events_csv(text)
        except UnicodeDecodeError:
            return None, fail("CSV 需为 UTF-8 编码")
        except CsvError as exc:
            # 坏行：在校验阶段拒绝，尚未写库，现用账与锚点原样保留
            return None, fail(f"CSV 校验失败：{exc}")
        return (upload.filename, parsed), None

    # ---------- 页面 ----------
    @app.get("/")
    def index():
        return render_template("index.html", min_anchors=MIN_ANCHORS)

    # ---------- 状态 ----------
    @app.get("/api/state")
    def state():
        conn = get_db()
        try:
            return ok(
                dataset=dbmod.get_dataset_meta(conn),
                anchors=dbmod.list_anchors(conn),
                pending_imports=dbmod.list_pending_imports(conn),
                datasets=dbmod.list_datasets(conn),
                min_anchors=MIN_ANCHORS,
            )
        finally:
            conn.close()

    @app.get("/api/events")
    def events():
        conn = get_db()
        try:
            if not dbmod.has_dataset(conn):
                return fail("尚未上传事件 CSV", 404)
            limit = request.args.get("limit", type=int)
            return ok(events=dbmod.list_events(conn, limit=limit))
        finally:
            conn.close()

    # ---------- 两阶段换账：阶段 1，暂存（整份校验通过后才写库，不碰现用账） ----------
    @app.post("/api/imports/start")
    def start_import():
        parsed, err = read_upload()
        if err is not None:
            return err
        filename, events = parsed

        conn = get_db()
        try:
            expected = dbmod.get_active_dataset_id(conn)
            try:
                staged = dbmod.stage_import(
                    conn, filename, [ev["count"] for ev in events], expected
                )
            except sqlite3.Error as exc:
                return fail(f"暂存写入失败，现用账与锚点保留：{exc}", 500)
            return ok(
                import_meta=staged,
                expected_active_id=expected,
                message=(
                    "新账已暂存并通过整份校验；导入期间现用账仍可核查与补登，"
                    "确认后提交启用"
                ),
            )
        finally:
            conn.close()

    # ---------- 阶段 2：提交启用（写锁内 O(1) 指针切换） ----------
    @app.post("/api/imports/<int:import_id>/commit")
    def commit_import(import_id: int):
        conn = get_db()
        try:
            try:
                dbmod.commit_import(conn, import_id)
            except dbmod.ImportConflict as exc:
                # 换账冲突：退回本次换账，现用账与锚点保留
                return fail(f"换账冲突：{exc}", 409, import_id=import_id)
            except LookupError as exc:
                return fail(str(exc), 404)
            except sqlite3.Error as exc:
                return fail(f"写入失败，现用账与锚点保留：{exc}", 500)
            return ok(
                dataset=dbmod.get_dataset_meta(conn),
                anchors=dbmod.list_anchors(conn),
                pending_imports=dbmod.list_pending_imports(conn),
                message="新账已启用，旧账与旧锚点已退出核查",
            )
        finally:
            conn.close()

    @app.delete("/api/imports/<int:import_id>")
    def discard_import(import_id: int):
        conn = get_db()
        try:
            removed = dbmod.discard_import(conn, import_id)
            if not removed:
                return fail("暂存导入不存在或已处理", 404)
            return ok(pending_imports=dbmod.list_pending_imports(conn))
        finally:
            conn.close()

    # ---------- 一次性换账（保留原接口：暂存 + 立即提交） ----------
    @app.post("/api/dataset")
    def upload_dataset():
        parsed, err = read_upload()
        if err is not None:
            return err
        filename, events = parsed

        conn = get_db()
        try:
            try:
                meta = dbmod.replace_dataset(
                    conn, filename, [ev["count"] for ev in events]
                )
            except dbmod.ImportConflict as exc:
                return fail(f"换账冲突：{exc}", 409)
            except sqlite3.Error as exc:
                return fail(f"写入失败，现用账与锚点保留：{exc}", 500)
            return ok(
                dataset=meta,
                anchors=dbmod.list_anchors(conn),
                message="事件账已重建，旧锚点随旧账退出核查",
            )
        finally:
            conn.close()

    # ---------- 锚点登记（导入/核查期间可随时补登记，始终落在现用账） ----------
    @app.post("/api/anchors")
    def create_anchor():
        payload = request.get_json(silent=True) or {}
        try:
            event_no = int(payload["event_no"])
            utc_ms = int(payload["utc_ms"])
        except (KeyError, TypeError, ValueError):
            return fail("event_no 与 utc_ms 必须是整数")
        if utc_ms < 0:
            return fail("UTC 毫秒不能为负")

        conn = get_db()
        try:
            if not dbmod.has_dataset(conn):
                return fail("请先上传事件 CSV")
            if not dbmod.event_exists(conn, event_no):
                return fail(f"事件号 {event_no} 不在事件账内")
            try:
                with conn:
                    anchor = dbmod.add_anchor(conn, event_no, utc_ms)
            except sqlite3.IntegrityError:
                return fail(f"事件号 {event_no} 已登记锚点，原账未改动", 409)
            return ok(anchor=anchor, anchors=dbmod.list_anchors(conn))
        finally:
            conn.close()

    @app.delete("/api/anchors/<int:anchor_id>")
    def remove_anchor(anchor_id: int):
        conn = get_db()
        try:
            removed = dbmod.delete_anchor(conn, anchor_id)
            conn.commit()
            if not removed:
                return fail("锚点不存在", 404)
            return ok(anchors=dbmod.list_anchors(conn))
        finally:
            conn.close()

    # ---------- 核查：事件与锚点取自同一只读快照（同一已提交状态） ----------
    @app.post("/api/verify")
    def run_verify():
        conn = get_db()
        try:
            snapshot = dbmod.read_snapshot(conn)
            if snapshot is None:
                return fail("请先上传事件 CSV")
            active_id, _meta, records, anchors = snapshot
            result = verify(records, anchors)
            return ok(result=result, anchors=anchors, dataset_id=active_id)
        finally:
            conn.close()

    # ---------- 确认更正：按当时全部锚点复核，冲突/失败保留原账 ----------
    @app.post("/api/correct")
    def confirm_correction():
        payload = request.get_json(silent=True) or {}
        try:
            anchor_id = int(payload["anchor_id"])
            new_utc = int(payload["new_utc_ms"])
        except (KeyError, TypeError, ValueError):
            return fail("anchor_id 与 new_utc_ms 必须是整数")
        if new_utc < 0:
            return fail("UTC 毫秒不能为负")

        conn = get_db()
        try:
            active_id = dbmod.get_active_dataset_id(conn)
            if active_id is None:
                return fail("请先上传事件 CSV")
            anchor = dbmod.get_anchor(conn, anchor_id)
            if anchor is None or anchor["dataset_id"] != active_id:
                return fail("锚点不存在或已随旧账退出核查", 404)

            result = None
            try:
                # 单写事务：改写 -> 按当时全部锚点复核 -> 不一致/异常则回滚。
                # 事务内读取的事件与锚点必然是同一已提交状态。
                conn.execute("BEGIN IMMEDIATE")
                dbmod.update_anchor_utc(conn, anchor_id, new_utc)
                records = dbmod.load_records(conn, active_id)
                anchors_now = [
                    dict(r)
                    for r in conn.execute(
                        "SELECT id, event_no, utc_ms FROM anchor"
                        " WHERE dataset_id = ? ORDER BY event_no",
                        (active_id,),
                    )
                ]
                result = verify(records, anchors_now)
                if result["status"] != "consistent":
                    raise RecheckConflict("复核未通过：新时刻无法让全账一致")
                conn.execute("COMMIT")
            except sqlite3.Error as exc:
                conn.execute("ROLLBACK")
                return fail(f"保存失败，已保留原账：{exc}", 500)
            except RecheckConflict as exc:
                conn.execute("ROLLBACK")
                return fail(
                    f"{exc}，更正已撤销",
                    409,
                    result=result,
                    original_utc_ms=anchor["utc_ms"],
                )
            except Exception:
                # 复核本身意外报错：回滚并按系统故障上报，绝不留下半提交状态
                conn.execute("ROLLBACK")
                raise
            return ok(
                result=result,
                anchors=dbmod.list_anchors(conn),
                corrected={
                    "anchor_id": anchor_id,
                    "event_no": anchor["event_no"],
                    "old_utc_ms": anchor["utc_ms"],
                    "new_utc_ms": new_utc,
                },
                message="更正已保存，全账复核一致",
            )
        finally:
            conn.close()

    return app
