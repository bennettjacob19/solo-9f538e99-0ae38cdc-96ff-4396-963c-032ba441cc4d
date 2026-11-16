"""启动登记缺口与重启恢复的进程内测试（真实 SQLite + 真实进程）。

模拟"API 被强杀"的方式：直接抛弃第一批 Database/Scheduler 对象，
不做任何清理（等价于 SIGKILL 后来不及跑任何 finally），
再用新对象在同一数据目录上执行 recover()。
"""
from __future__ import annotations

import asyncio
import os
import signal
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from app import config, proc
from app.db import Database
from app.models import BuildCreate, StepIn
from app.scheduler import Scheduler


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    """把全部路径/时序配置隔离到临时目录。"""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "pipeline.db")
    monkeypatch.setattr(config, "WORKDIR_ROOT", tmp_path / "workdir")
    monkeypatch.setattr(config, "LOG_ROOT", tmp_path / "logs")
    monkeypatch.setattr(config, "TERM_GRACE_SECONDS", 0.3)
    monkeypatch.setattr(config, "KILL_GRACE_SECONDS", 10.0)
    monkeypatch.setattr(config, "PROC_POLL_INTERVAL", 0.02)
    monkeypatch.setattr(config, "SCHEDULER_TICK", 0.02)
    monkeypatch.setattr(config, "SPAWN_REGISTER_DELAY_SECONDS", 0.0)
    return tmp_path


async def _wait_build_terminal(db: Database, build_id: int, timeout: float = 30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await db.get_build(build_id)
        if row["status"] in ("succeeded", "failed", "cancelled", "interrupted"):
            return row
        await asyncio.sleep(0.05)
    raise AssertionError(f"构建 {build_id} 未在 {timeout}s 内到达终态")


def _marked_pids(token: str) -> list[int]:
    return [m.pid for m in proc.scan_marked_processes() if m.token == token]


def _pid_alive(pid: int) -> bool:
    return Path(f"/proc/{pid}").exists()


# ------------------------------------------------------- 旧数据卷迁移

_OLD_SCHEMA = """
CREATE TABLE builds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL, status TEXT NOT NULL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    detail TEXT, created_at TEXT NOT NULL,
    started_at TEXT, finished_at TEXT
);
CREATE TABLE steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    build_id INTEGER NOT NULL, step_uid TEXT NOT NULL,
    command TEXT NOT NULL, depends_on TEXT NOT NULL,
    timeout_seconds REAL NOT NULL, status TEXT NOT NULL,
    exit_code INTEGER, detail TEXT, log_path TEXT,
    pid INTEGER, pgid INTEGER, proc_starttime REAL,
    started_at TEXT, finished_at TEXT,
    UNIQUE(build_id, step_uid)
);
"""


def test_old_volume_migrates_and_stays_readable(isolated):
    # 手工造一个 1.0 时代的库（无 start_token 列）并放入数据
    config.ensure_dirs()
    conn = sqlite3.connect(config.DB_PATH)
    conn.executescript(_OLD_SCHEMA)
    conn.execute(
        "INSERT INTO builds (name, status, created_at) VALUES ('legacy', 'succeeded', ?)",
        (_now(),),
    )
    conn.execute(
        "INSERT INTO steps (build_id, step_uid, command, depends_on, "
        "timeout_seconds, status, log_path) VALUES (1, 'compile', 'true', '[]', 5, 'succeeded', '/tmp/x.log')"
    )
    conn.commit()
    conn.close()

    async def main():
        db = Database()
        await db.connect()  # 应就地 ALTER TABLE，不丢数据
        try:
            build = await db.get_build(1)
            assert build["status"] == "succeeded"
            steps = await db.get_steps(1)
            assert steps[0]["step_uid"] == "compile"
            assert steps[0]["start_token"] is None  # 旧行令牌为空
            # 新功能在旧卷上可用
            await db.set_step_starting(steps[0]["id"], "tok-1", _now())
            refreshed = (await db.get_steps(1))[0]
            assert refreshed["start_token"] == "tok-1"
            assert refreshed["status"] == "starting"
        finally:
            await db.close()

    asyncio.run(main())


# --------------------------------------- 身份登记失败：已产生的进程必须回收

def test_registration_failure_reclaims_spawned_process(isolated, monkeypatch):
    async def main():
        db = Database()
        await db.connect()
        sched = Scheduler(db)

        async def boom(*args, **kwargs):
            raise RuntimeError("模拟 SQLite 写失败")

        monkeypatch.setattr(db, "set_step_process", boom)
        try:
            payload = BuildCreate(
                name="reg-fail",
                steps=[
                    StepIn(
                        id="compile",
                        command="echo started > marker.txt; sleep 300",
                        timeout_seconds=30,
                    )
                ],
            )
            build_id = await sched.submit(payload)
            build = await _wait_build_terminal(db, build_id)
            assert build["status"] == "failed"
            step_row = (await db.get_steps(build_id))[0]
            assert step_row["status"] == "failed"
            assert "身份登记失败" in (step_row["detail"] or "")
            token = step_row["start_token"]
            assert token  # 令牌已落库
            # 关键断言：登记失败时产生的进程已被回收，没有漏网
            assert _marked_pids(token) == []
            log = Path(step_row["log_path"]).read_text()
            assert "身份登记失败" in log and "回收" in log
        finally:
            await db.close()

    asyncio.run(main())


# ----------------- 强杀于"已启动未登记"窗口：恢复必须清理孤儿进程组

def test_recover_cleans_orphan_from_registration_gap(isolated):
    async def main():
        db = Database()
        await db.connect()
        build_id = await db.create_build("gap-victim", _now())
        log_dir = config.LOG_ROOT / str(build_id)
        log_dir.mkdir(parents=True, exist_ok=True)
        compile_log = log_dir / "compile.log"
        compile_log.touch()
        package_log = log_dir / "package.log"
        package_log.touch()
        await db.create_step(build_id, "compile", "sleep 300", [], 300, str(compile_log))
        await db.create_step(
            build_id, "package", "echo packed > pkg.txt", ["compile"], 30, str(package_log)
        )
        await db.set_build_status(build_id, "running", _now(), started=True)
        steps = await db.get_steps(build_id)
        compile_pk = next(s["id"] for s in steps if s["step_uid"] == "compile")

        # 现场还原：令牌已落库、进程已 spawn、身份尚未写入 —— 此刻"强杀"
        token = uuid4().hex
        await db.set_step_starting(compile_pk, token, _now())
        workdir = config.WORKDIR_ROOT / str(build_id)
        workdir.mkdir(parents=True, exist_ok=True)
        env = {
            **os.environ,
            proc.ENV_BUILD_ID: str(build_id),
            proc.ENV_STEP_ID: "compile",
            proc.ENV_STEP_TOKEN: token,
        }
        log_f = open(compile_log, "ab")
        orphan, orphan_pid, _, _ = await proc.spawn(
            "sleep 300", workdir, env, log_f, log_f
        )
        log_f.close()
        await db.close()  # 第一批对象就此抛弃，等价于进程被强杀

        # 同机"重启"：全新对象在同一数据目录上恢复
        db2 = Database()
        await db2.connect()
        try:
            sched2 = Scheduler(db2)
            report = await sched2.recover()
            assert report["builds"] == 1
            assert report["killed_groups"] == 1

            # 孤儿进程组已被清空
            await asyncio.wait_for(orphan.wait(), timeout=10)
            assert not _pid_alive(orphan_pid)
            assert _marked_pids(token) == []

            # 状态确认：全部 interrupted，未运行步骤不补跑
            build = await db2.get_build(build_id)
            assert build["status"] == "interrupted"
            final = {s["step_uid"]: s for s in await db2.get_steps(build_id)}
            assert final["compile"]["status"] == "interrupted"
            assert final["package"]["status"] == "interrupted"
            assert not (workdir / "pkg.txt").exists()
            # 恢复可重入：再次执行安全无副作用
            assert (await sched2.recover()) == {"builds": 0, "killed_groups": 0}
        finally:
            await db2.close()

    asyncio.run(main())


# ---------- 旧版本数据卷：无令牌进程凭 (build, step) 环境对识别清理

def test_recover_cleans_legacy_orphan_without_token(isolated):
    """还原旧版本的漏网现场：步骤行停在 pending、无令牌、无身份，
    但进程真实存在（只带 PIPELINE_BUILD_ID/PIPELINE_STEP_ID 环境）。
    新恢复必须凭环境对找回并清理它。"""

    async def main():
        db = Database()
        await db.connect()
        build_id = await db.create_build("legacy-gap", _now())
        log_dir = config.LOG_ROOT / str(build_id)
        log_dir.mkdir(parents=True, exist_ok=True)
        log = log_dir / "compile.log"
        log.touch()
        await db.create_step(build_id, "compile", "sleep 300", [], 300, str(log))
        await db.set_build_status(build_id, "running", _now(), started=True)
        # 旧版本进程的环境：只有 BUILD_ID/STEP_ID，没有令牌
        workdir = config.WORKDIR_ROOT / str(build_id)
        workdir.mkdir(parents=True, exist_ok=True)
        env = {
            **os.environ,
            proc.ENV_BUILD_ID: str(build_id),
            proc.ENV_STEP_ID: "compile",
        }
        log_f = open(log, "ab")
        orphan, orphan_pid, _, _ = await proc.spawn(
            "sleep 300", workdir, env, log_f, log_f
        )
        log_f.close()
        await db.close()  # "强杀"

        db2 = Database()
        await db2.connect()
        try:
            report = await Scheduler(db2).recover()
            assert report["killed_groups"] == 1
            await asyncio.wait_for(orphan.wait(), timeout=10)
            assert not _pid_alive(orphan_pid)
            step_row = (await db2.get_steps(build_id))[0]
            assert step_row["status"] == "interrupted"
            assert step_row["start_token"] is None
            log_text = log.read_text()
            assert "环境标记" in log_text
        finally:
            await db2.close()

    asyncio.run(main())


# ----------------- 不误杀：终态构建、陌生标记、无标记进程都不动

def test_recover_never_kills_unrelated_processes(isolated):
    victims: list[subprocess.Popen] = []

    def spawn_marked(env_extra: dict[str, str]) -> subprocess.Popen:
        p = subprocess.Popen(
            ["/bin/sh", "-c", "sleep 300"],
            env={**os.environ, **env_extra},
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        victims.append(p)
        return p

    async def main():
        db = Database()
        await db.connect()
        # 终态构建：即使有同名标记进程存活，恢复也绝不动它
        done_id = await db.create_build("done", _now())
        await db.set_build_status(done_id, "succeeded", _now(), finished=True)
        terminal_proc = spawn_marked(
            {proc.ENV_BUILD_ID: str(done_id), proc.ENV_STEP_ID: "compile"}
        )
        # 非终态构建，但进程标记的步骤在库中不存在（陌生标记）
        live_id = await db.create_build("live", _now())
        log_dir = config.LOG_ROOT / str(live_id)
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "a.log").touch()
        await db.create_step(live_id, "a", "true", [], 5, str(log_dir / "a.log"))
        await db.set_build_status(live_id, "running", _now(), started=True)
        ghost_proc = spawn_marked(
            {proc.ENV_BUILD_ID: str(live_id), proc.ENV_STEP_ID: "ghost"}
        )
        plain_proc = spawn_marked({})  # 完全无标记的无关进程
        await db.close()

        db2 = Database()
        await db2.connect()
        try:
            report = await Scheduler(db2).recover()
            assert report["builds"] == 1  # 只有 live 被处理
            assert report["killed_groups"] == 0  # 没有任何进程被清理
            build = await db2.get_build(live_id)
            assert build["status"] == "interrupted"
        finally:
            await db2.close()
        return terminal_proc, ghost_proc, plain_proc

    try:
        for p in asyncio.run(main()):
            assert _pid_alive(p.pid), f"无关进程 {p.pid} 被恢复逻辑误杀"
    finally:
        for p in victims:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            p.wait()
