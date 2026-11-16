"""进程标记扫描与残留清理原语的单元测试。"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import time
from pathlib import Path

from app import proc


def _spawn_marked(env_extra: dict[str, str], command: str = "sleep 60") -> subprocess.Popen:
    return subprocess.Popen(
        ["/bin/sh", "-c", command],
        env={**os.environ, **env_extra},
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _alive(pid: int) -> bool:
    return Path(f"/proc/{pid}").exists()


def _kill_quiet(p: subprocess.Popen) -> None:
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    p.wait()


def test_read_pgrp_matches_getpgid():
    p = _spawn_marked({})
    try:
        assert proc.read_pgrp(p.pid) == os.getpgid(p.pid) == p.pid
    finally:
        _kill_quiet(p)
    assert proc.read_pgrp(p.pid) is None  # 进程消失后返回 None


def test_scan_finds_only_marked_processes():
    token = "testtoken123"
    marked = _spawn_marked(
        {
            proc.ENV_BUILD_ID: "42",
            proc.ENV_STEP_ID: "compile",
            proc.ENV_STEP_TOKEN: token,
        }
    )
    plain = _spawn_marked({})
    try:
        time.sleep(0.2)  # 等 exec 完成、environ 稳定
        found = {m.pid: m for m in proc.scan_marked_processes()}
        assert marked.pid in found
        assert plain.pid not in found  # 无标记进程绝不入列
        m = found[marked.pid]
        assert m.build_id == "42" and m.step_uid == "compile" and m.token == token
        assert m.pgrp == marked.pid
    finally:
        _kill_quiet(marked)
        _kill_quiet(plain)


def test_terminate_residual_processes_kills_stubborn_group():
    # 顽固进程组：忽略 SIGTERM，必须升级到 SIGKILL 才能清空
    token = "stubborn-token-1"
    p = _spawn_marked(
        {proc.ENV_STEP_TOKEN: token},
        command="trap '' TERM; sleep 300 & sleep 300",
    )

    def find() -> list[proc.Marker]:
        return [m for m in proc.scan_marked_processes() if m.token == token]

    async def run() -> bool:
        logs: list[str] = []
        return await proc.terminate_residual_processes(
            find, "测试清理", logs, term_grace=0.3, kill_grace=10, poll_interval=0.05
        )

    try:
        time.sleep(0.2)
        assert asyncio.run(run()) is True
        # 本测试是被杀进程的父进程：先回收僵尸再断言消失
        for _ in range(50):
            if p.poll() is not None:
                break
            time.sleep(0.1)
        assert p.poll() is not None
        assert not _alive(p.pid)
    finally:
        if _alive(p.pid):
            _kill_quiet(p)


def test_terminate_residual_processes_noop_when_clean():
    logs: list[str] = []

    async def run() -> bool:
        return await proc.terminate_residual_processes(
            lambda: [], "测试清理", logs, term_grace=0.1, kill_grace=0.5,
            poll_interval=0.02,
        )

    assert asyncio.run(run()) is True
    assert logs == []  # 没有残留时不产生任何动作
