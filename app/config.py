"""全局配置，全部可通过环境变量覆盖，便于容器与测试使用。"""
from __future__ import annotations

import os
from pathlib import Path


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


# 数据根目录：SQLite、每构建工作目录、步骤日志都放在这里，compose 中挂载为卷
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", str(Path.cwd() / "data")))
DB_PATH = DATA_DIR / "pipeline.db"
WORKDIR_ROOT = DATA_DIR / "workdir"
LOG_ROOT = DATA_DIR / "logs"

# 业务约束
MAX_STEPS = int(os.environ.get("PIPELINE_MAX_STEPS", "12"))
MAX_PARALLEL = int(os.environ.get("PIPELINE_MAX_PARALLEL", "2"))
TIMEOUT_CAP_SECONDS = _env_float("PIPELINE_TIMEOUT_CAP_SECONDS", "86400")

# 进程组清理时序：先 SIGTERM 宽限，再 SIGKILL 硬等；硬等有界，避免永久挂死
TERM_GRACE_SECONDS = _env_float("PIPELINE_TERM_GRACE_SECONDS", "5")
KILL_GRACE_SECONDS = _env_float("PIPELINE_KILL_GRACE_SECONDS", "60")

# 轮询间隔（秒）
PROC_POLL_INTERVAL = _env_float("PIPELINE_PROC_POLL_INTERVAL", "0.1")
SCHEDULER_TICK = _env_float("PIPELINE_SCHEDULER_TICK", "0.2")

# 故障注入（仅测试用）：在"进程已 spawn、身份未落库"的窗口内人为睡眠，
# 用于复现并验证"启动登记窗口期被强杀"的恢复行为。生产环境必须为 0。
SPAWN_REGISTER_DELAY_SECONDS = _env_float("PIPELINE_SPAWN_REGISTER_DELAY_SECONDS", "0")

# 日志接口单次最多返回字节数
LOG_MAX_BYTES = int(os.environ.get("PIPELINE_LOG_MAX_BYTES", str(1024 * 1024)))


def ensure_dirs() -> None:
    for path in (DATA_DIR, WORKDIR_ROOT, LOG_ROOT):
        path.mkdir(parents=True, exist_ok=True)
