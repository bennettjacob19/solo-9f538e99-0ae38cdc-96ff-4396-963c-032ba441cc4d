"""端到端集成测试：以真实 uvicorn 子进程启动 API，覆盖全部关键链路。

运行:
  .venv/bin/pytest tests/test_integration.py -v
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
TERMINAL = {"succeeded", "failed", "cancelled", "interrupted"}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ServerHarness:
    __test__ = False  # 非测试类，避免 pytest 误收集

    def __init__(self, data_dir: Path, port: int, extra_env: dict | None = None):
        self.data_dir = data_dir
        self.port = port
        self.extra_env = extra_env or {}
        self.proc: subprocess.Popen | None = None
        self.log_file: Path | None = None
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30)

    def start(self, wait: bool = True) -> None:
        env = {
            **os.environ,
            "PIPELINE_DATA_DIR": str(self.data_dir),
            "PIPELINE_TERM_GRACE_SECONDS": "0.5",
            "PIPELINE_KILL_GRACE_SECONDS": "10",
            "PIPELINE_PROC_POLL_INTERVAL": "0.05",
            "PIPELINE_SCHEDULER_TICK": "0.05",
            **self.extra_env,
        }
        # 输出落文件而不是 PIPE：管道缓冲写满会冻结 uvicorn 事件循环
        self.log_file = self.data_dir / "server.log"
        log_fh = open(self.log_file, "ab")
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--workers",
                "1",
            ],
            cwd=ROOT,
            env=env,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        if wait:
            self.wait_ready()

    def wait_ready(self) -> None:
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                r = httpx.get(f"http://127.0.0.1:{self.port}/health", timeout=1)
                if r.status_code == 200:
                    return
            except httpx.TransportError:
                pass
            if self.proc.poll() is not None:
                out = self.log_file.read_text() if self.log_file else ""
                raise RuntimeError(f"服务器提前退出:\n{out}")
            time.sleep(0.1)
        raise RuntimeError(f"服务器启动超时，日志:\n{self.log_file.read_text()}")

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.proc is None:
            return
        try:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        finally:
            self.client.close()
            self.proc = None


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = ServerHarness(tmp_path_factory.mktemp("data"), _free_port())
    srv.start()
    yield srv
    srv.stop()


def step(uid, command, deps=None, timeout=30.0):
    return {"id": uid, "command": command, "depends_on": deps or [],
            "timeout_seconds": timeout}


def submit(client: httpx.Client, name: str, steps: list[dict]) -> int:
    r = client.post("/builds", json={"name": name, "steps": steps})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def wait_build(client: httpx.Client, build_id: int, timeout: float = 40) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        detail = client.get(f"/builds/{build_id}").json()
        if detail["status"] in TERMINAL:
            return detail
        time.sleep(0.1)
    raise AssertionError(f"构建 {build_id} 未在 {timeout}s 内结束")


def step_map(detail: dict) -> dict:
    return {s["id"]: s for s in detail["steps"]}


def assert_pid_dead(pid: int) -> None:
    for _ in range(30):
        if not Path(f"/proc/{pid}").exists():
            return
        time.sleep(0.1)
    raise AssertionError(f"进程 {pid} 在清理后仍然存活")


def command_output(log_payload: dict) -> str:
    """剔除 [runner] 元数据行，只保留命令自身的 stdout/stderr。"""
    return "\n".join(
        line for line in log_payload["content"].splitlines()
        if not line.startswith("[runner]")
    )


# --------------------------------------------------------------- 1. 成功构建

def test_success_build_runs_all_steps(server):
    bid = submit(server.client, "success", [
        step("prepare", "mkdir -p out && echo v1 > out/fw.txt"),
        step("compile", "test -f out/fw.txt && echo compiled", ["prepare"]),
        step("package", "echo packed", ["compile"]),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "succeeded"
    assert all(s["status"] == "succeeded" for s in detail["steps"])
    artifact = server.data_dir / "workdir" / str(bid) / "out" / "fw.txt"
    assert artifact.read_text().strip() == "v1"
    log = server.client.get(f"/builds/{bid}/steps/compile/logs").json()
    assert "compiled" in log["content"]
    assert log["status"] == "succeeded"


# ----------------------------------------------------- 2. 失败跳过 + 独立分支

def test_failure_skips_descendants_but_independent_branch_continues(server):
    bid = submit(server.client, "failure", [
        step("good", "echo ok"),
        step("bad", "echo boom; exit 7"),
        step("child", "echo should-not-run", ["bad"]),
        step("grand", "echo should-not-run", ["child"]),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "failed"
    sm = step_map(detail)
    assert sm["good"]["status"] == "succeeded"
    assert sm["bad"]["status"] == "failed" and sm["bad"]["exit_code"] == 7
    assert sm["child"]["status"] == "skipped"
    assert sm["grand"]["status"] == "skipped"
    log = server.client.get(f"/builds/{bid}/steps/child/logs").json()
    assert "should-not-run" not in command_output(log)  # 跳过的步骤确实没执行


# ------------------------------------------------- 3. 慢子进程：主退出后判失败

def test_lingering_child_is_killed_and_build_fails(server):
    bid = submit(server.client, "slow-child", [
        step(
            "spawn",
            # 不用子 shell 包装，$! 才能拿到真实后台子进程 PID
            "sleep 300 & echo $! > slow.pid && echo main-done && exit 0",
            timeout=10,
        ),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "failed"
    sm = step_map(detail)
    assert sm["spawn"]["status"] == "failed"
    assert "遗留子进程" in (sm["spawn"]["detail"] or "")
    workdir = server.data_dir / "workdir" / str(bid)
    child_pid = int((workdir / "slow.pid").read_text().strip())
    assert_pid_dead(child_pid)  # 进程组清空前不确认终态：此时必须已死
    log = server.client.get(f"/builds/{bid}/steps/spawn/logs").json()
    assert "进程组" in log["content"] and "清空" in log["content"]


# ------------------------------------------------------------------- 4. 超时

def test_timeout_kills_group_and_marks_failed(server):
    bid = submit(server.client, "timeout", [
        step("slow", "echo start; sleep 30; echo end", timeout=1),
    ])
    detail = wait_build(server.client, bid, timeout=20)
    assert detail["status"] == "failed"
    assert step_map(detail)["slow"]["status"] == "failed"
    assert "超时" in (step_map(detail)["slow"]["detail"] or "")
    log = server.client.get(f"/builds/{bid}/steps/slow/logs").json()
    out = command_output(log)
    assert "start" in out
    assert "end" not in out  # 超时点在 sleep，后续输出不会产生


# ------------------------------------------------------ 5. 全局最多两步并行

def test_global_parallelism_capped_at_two(server):
    # 四个各 1.2s 的独立步骤，2 名额 -> 至少两批，总耗时应 >= 2.0s 且 < 4.5s
    steps = [step(f"s{i}", "echo running-$PIPELINE_STEP_ID; sleep 1.2") for i in range(4)]
    t0 = time.time()
    bid = submit(server.client, "parallel-cap", steps)
    detail = wait_build(server.client, bid, timeout=30)
    elapsed = time.time() - t0
    assert detail["status"] == "succeeded"
    assert elapsed >= 2.0, f"并行度疑似超过 2，总耗时仅 {elapsed:.2f}s"
    assert elapsed < 4.5


# ------------------------------------------------------------- 6. 整次取消

def test_cancel_stops_all_running_steps_and_skips_pending(server):
    bid = submit(server.client, "cancel", [
        step("a", "echo a-start; sleep 20; echo a-end"),
        step("b", "echo b-start; sleep 20; echo b-end"),
        step("c", "echo c-should-not-run; sleep 20", ["a"]),
    ])
    # 等两个名额占满
    deadline = time.time() + 10
    running = []
    while time.time() < deadline:
        sm = step_map(server.client.get(f"/builds/{bid}").json())
        running = [k for k, v in sm.items() if v["status"] == "running"]
        if len(running) == 2:
            break
        time.sleep(0.1)
    assert sorted(running) == ["a", "b"]

    r = server.client.post(f"/builds/{bid}/cancel")
    assert r.status_code == 200
    detail = wait_build(server.client, bid, timeout=20)
    sm = step_map(detail)
    assert detail["status"] == "cancelled"
    assert sm["a"]["status"] == "cancelled" and sm["b"]["status"] == "cancelled"
    assert sm["c"]["status"] == "cancelled"
    for uid in ("a", "b"):
        log = server.client.get(f"/builds/{bid}/steps/{uid}/logs").json()
        out = command_output(log)
        assert f"{uid}-start" in out
        assert f"{uid}-end" not in out  # 取消点在 sleep，尾部输出不会产生


def test_repeated_cancel_is_idempotent(server):
    bid = submit(server.client, "cancel-idem", [step("a", "sleep 20")])
    time.sleep(0.5)
    first = server.client.post(f"/builds/{bid}/cancel")
    assert first.status_code == 200
    detail1 = wait_build(server.client, bid, timeout=20)
    second = server.client.post(f"/builds/{bid}/cancel")
    assert second.status_code == 200
    assert "终态" in second.json()["detail"]
    third = server.client.get(f"/builds/{bid}").json()
    assert third["status"] == detail1["status"] == "cancelled"
    assert third["finished_at"] == detail1["finished_at"]  # 终态未被改写


# --------------------------------------- 7. 慢清理不阻塞另一名额（跨构建）

def test_slow_cleanup_does_not_block_other_slot(server):
    # 顽固子进程忽略 SIGTERM（忽略型信号处置会被子进程继承），
    # 主命令退出后清理必须升级到 SIGKILL 流程
    stubborn = (
        "trap '' TERM; sleep 300 & echo $! > stubborn.pid; "
        "echo main-exit; exit 0"
    )
    t0 = time.time()
    bid_busy = submit(server.client, "slow-cleanup", [step("x", stubborn, timeout=10)])
    time.sleep(0.6)  # 确保 busy 已占住一个名额
    bid_fast = submit(server.client, "other-slot", [step("y", "echo fast-ok")])
    detail = wait_build(server.client, bid_fast, timeout=15)
    fast_elapsed = time.time() - t0
    assert detail["status"] == "succeeded"
    # 另一名额可用：快构建在 5s 内完成（被慢清理拖住则要等 KILL 流程更久）
    assert fast_elapsed < 5, f"另一名额疑似被慢清理阻塞: {fast_elapsed:.2f}s"
    busy = wait_build(server.client, bid_busy, timeout=30)
    assert busy["status"] == "failed"  # 顽固子进程最终被 SIGKILL，仍判失败
    workdir = server.data_dir / "workdir" / str(bid_busy)
    pid = int((workdir / "stubborn.pid").read_text().strip())
    assert_pid_dead(pid)


# --------------------------------------------------------- 8. 重启恢复全链路

def test_restart_recovers_and_does_not_kill_new_build(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("restart-data")
    port = _free_port()
    srv = ServerHarness(data_dir, port)
    srv.start()
    try:
        bid = submit(srv.client, "will-be-interrupted", [
            step("a", "sleep 30"),
            step("b", "sleep 30"),
            step("c", "echo never", ["a"]),
        ])
        deadline = time.time() + 10
        while time.time() < deadline:
            sm = step_map(srv.client.get(f"/builds/{bid}").json())
            if sm["a"]["status"] == "running":
                running_pid = _step_pid(data_dir, bid, "a")
                break
            time.sleep(0.1)
        else:
            raise AssertionError("步骤未进入 running")
        assert Path(f"/proc/{running_pid}").exists()
        srv.stop()
        # 进程组随 API 被 tini 之外的场景保留（同会话 SIGTERM 只停 uvicorn）
        assert Path(f"/proc/{running_pid}").exists()
    finally:
        pass

    # 同数据目录重启：先清理残留，再标记 interrupted，然后才对外服务
    srv2 = ServerHarness(data_dir, port)
    srv2.start()
    try:
        assert_pid_dead(running_pid)  # 旧构建残留必须被清掉
        detail = srv2.client.get(f"/builds/{bid}").json()
        assert detail["status"] == "interrupted"
        sm = step_map(detail)
        assert sm["a"]["status"] == "interrupted"
        assert sm["b"]["status"] == "interrupted"
        assert sm["c"]["status"] == "interrupted"  # 未跑步骤不再启动

        # 后来的构建必须正常工作，不得被恢复逻辑误杀
        new_id = submit(srv2.client, "after-restart", [
            step("z", "echo alive && test $PIPELINE_BUILD_ID -ge 1"),
        ])
        new_detail = wait_build(srv2.client, new_id)
        assert new_detail["status"] == "succeeded"
    finally:
        srv2.stop()


def _step_pid(data_dir: Path, build_id: int, step_uid: str) -> int:
    import sqlite3

    conn = sqlite3.connect(data_dir / "pipeline.db")
    try:
        row = conn.execute(
            "SELECT pid FROM steps WHERE build_id=? AND step_uid=?",
            (build_id, step_uid),
        ).fetchone()
        return int(row[0])
    finally:
        conn.close()


# --------------------------------------- 10. 强杀于"已启动未登记"窗口（固件团队上报场景）

def _step_row(data_dir: Path, build_id: int, step_uid: str) -> dict | None:
    import sqlite3

    conn = sqlite3.connect(data_dir / "pipeline.db")
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM steps WHERE build_id=? AND step_uid=?",
            (build_id, step_uid),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _find_pid_by_token(token: str) -> int | None:
    from app.proc import scan_marked_processes

    for m in scan_marked_processes():
        if m.token == token:
            return m.pid
    return None


def test_kill_between_spawn_and_registration_recovers(tmp_path_factory):
    """编译命令已启动、身份尚未落库时 SIGKILL：重启后孤儿进程组必须被清理，
    工作目录不得再被旧命令修改，新构建正常放行。"""
    data_dir = tmp_path_factory.mktemp("gap-data")
    port = _free_port()
    # 故障注入：把"已启动未登记"窗口拉大到 10s，确保 SIGKILL 落在窗口内
    srv = ServerHarness(
        data_dir, port, extra_env={"PIPELINE_SPAWN_REGISTER_DELAY_SECONDS": "10"}
    )
    srv.start()
    try:
        bid = submit(srv.client, "killed-mid-registration", [
            step(
                "compile",
                "for i in $(seq 1 900); do echo tick-$i >> ticks.txt; sleep 0.2; done",
                timeout=300,
            ),
        ])
        # 等到"令牌已落库、身份未登记"（此时进程已 spawn）
        token = None
        deadline = time.time() + 15
        while time.time() < deadline:
            row = _step_row(data_dir, bid, "compile")
            if row and row["start_token"] and row["pid"] is None:
                token = row["start_token"]
                break
            time.sleep(0.05)
        assert token, "未能观察到启动登记窗口"
        # 窗口期内带令牌的进程确实存在
        orphan_pid = None
        deadline = time.time() + 5
        while time.time() < deadline:
            orphan_pid = _find_pid_by_token(token)
            if orphan_pid:
                break
            time.sleep(0.05)
        assert orphan_pid, "窗口期内应已存在带令牌的进程"
    finally:
        srv.stop(sig=signal.SIGKILL)  # 强杀，不给任何清理机会

    # 强杀后孤儿仍在跑、仍在写工作目录（旧版本里它会永远跑下去）
    assert Path(f"/proc/{orphan_pid}").exists()
    ticks = data_dir / "workdir" / str(bid) / "ticks.txt"

    # 同数据目录重启（无故障注入）：启动恢复必须先清残留再对外服务
    srv2 = ServerHarness(data_dir, port)
    srv2.start()
    try:
        assert_pid_dead(orphan_pid)  # 孤儿进程组已被清理
        detail = srv2.client.get(f"/builds/{bid}").json()
        assert detail["status"] == "interrupted"
        assert step_map(detail)["compile"]["status"] == "interrupted"
        # 旧命令不再修改工作目录
        assert ticks.exists()
        first = len(ticks.read_text().splitlines())
        assert first > 0
        time.sleep(1.0)
        assert len(ticks.read_text().splitlines()) == first
        # 名额不被幽灵占用：新构建正常跑通
        new_id = submit(srv2.client, "after-gap", [step("z", "echo alive")])
        assert wait_build(srv2.client, new_id)["status"] == "succeeded"
    finally:
        srv2.stop()


# --------------------------------------- 11. 恢复途中再次被强杀，仍能完成清理

def test_restart_during_recovery_still_cleans_up(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("recovery-kill-data")
    port = _free_port()
    # 顽固进程组：整体忽略 SIGTERM，恢复必须走到 SIGKILL 才能清空
    stubborn = "trap '' TERM; sleep 300 & sleep 300"
    srv1 = ServerHarness(data_dir, port)
    srv1.start()
    try:
        bid = submit(srv1.client, "stubborn-victim", [step("x", stubborn, timeout=300)])
        deadline = time.time() + 10
        victim_pid = None
        while time.time() < deadline:
            row = _step_row(data_dir, bid, "x")
            if row and row["pid"]:
                victim_pid = int(row["pid"])
                break
            time.sleep(0.05)
        assert victim_pid
        assert Path(f"/proc/{victim_pid}").exists()
    finally:
        srv1.stop(sig=signal.SIGKILL)
    assert Path(f"/proc/{victim_pid}").exists()

    # 第二次启动：把 SIGTERM 宽限拉大到 6s，在恢复清理途中再次强杀
    srv2 = ServerHarness(
        data_dir, port, extra_env={"PIPELINE_TERM_GRACE_SECONDS": "6"}
    )
    srv2.start(wait=False)  # 不等就绪：恢复完成前 /health 不可用
    try:
        time.sleep(2)  # 恢复已开始（TERM 已发出但被忽略），宽限远未结束
    finally:
        srv2.stop(sig=signal.SIGKILL)
    # 清理确实没有完成，顽固进程还活着
    assert Path(f"/proc/{victim_pid}").exists()

    # 第三次启动：恢复可重入，这次走完 TERM->KILL 全流程
    srv3 = ServerHarness(data_dir, port)
    srv3.start()
    try:
        assert_pid_dead(victim_pid)
        detail = srv3.client.get(f"/builds/{bid}").json()
        assert detail["status"] == "interrupted"
        assert step_map(detail)["x"]["status"] == "interrupted"
        # 恢复报告可见
        health = srv3.client.get("/health").json()
        assert health["recovery"]["builds"] >= 1
    finally:
        srv3.stop()


# --------------------------------------------------------------- 9. 其他接口

def test_404_and_bad_payloads(server):
    assert server.client.get("/builds/999999").status_code == 404
    assert server.client.post("/builds/999999/cancel").status_code == 404

    bad_cycle = {"name": "cycle", "steps": [
        step("a", "true", ["b"]), step("b", "true", ["a"])
    ]}
    r = server.client.post("/builds", json=bad_cycle)
    assert r.status_code == 400 and "循环" in r.json()["detail"]

    missing_dep = {"name": "ghost", "steps": [step("a", "true", ["x"])]}
    r = server.client.post("/builds", json=missing_dep)
    assert r.status_code == 400 and "不存在" in r.json()["detail"]

    empty = {"name": "empty", "steps": []}
    assert server.client.post("/builds", json=empty).status_code == 422


def test_health_and_docs(server):
    health = server.client.get("/health").json()
    assert health["status"] == "ok" and health["max_parallel"] == 2
    assert server.client.get("/docs").status_code == 200
    assert server.client.get("/openapi.json").status_code == 200
