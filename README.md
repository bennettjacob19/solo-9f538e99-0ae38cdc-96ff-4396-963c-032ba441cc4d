# 固件构建流水线 API

FastAPI + SQLite 实现的 DAG 构建流水线：接收至多 12 步的 JSON DAG，在挂载工作目录中
执行真实 shell 命令，全局最多 2 步并行，带依赖调度、取消、超时、进程组清理和重启恢复。

## 功能与语义对照

| 需求 | 实现 |
| --- | --- |
| 至多 12 步 JSON DAG，含命令/依赖/超时 | `POST /builds`，Pydantic 校验 + 重复 id / 悬空依赖 / 自依赖 / 有环检测（400/422） |
| 挂载工作目录执行真实命令 | 每构建独立目录 `$PIPELINE_DATA_DIR/workdir/<build_id>/`，compose 挂载命名卷 `/data` |
| 全局最多两步并行 | 单 worker 内 `asyncio.Semaphore(2)`，名额覆盖步骤全生命周期 |
| 依赖全成功才启动 | 调度器按 DAG 推进，依赖未全部 `succeeded` 不启动 |
| 失败使后代跳过，独立分支继续 | 上游出现失败/取消/中断即置后代 `skipped`；无依赖关系的分支不受影响 |
| 整次取消停止所有未完成步骤 | `POST /builds/{id}/cancel`，per-build 事件即时通知；未启动步骤不再启动 |
| 超时/取消终止对应进程组 | 每步 `start_new_session=True` 独立进程组，`killpg(PGID, SIGTERM→SIGKILL)` |
| 主命令退出后慢子进程清理并判失败 | 退出后扫描 `/proc` 检查组内残留，有残留即清理且步骤判 `failed` |
| 进程组清空前不释放名额/不确认终态 | 信号量在清理完成、终态落库后才释放；期间状态可见为 `cleaning_up` |
| 慢清理不阻塞另一名额 | 清理是异步等待，只占自己的名额，另一名额正常调度其他步骤/构建 |
| 重启先清理旧残留再标中断 | startup 恢复：PID+starttime 双重身份确认后终止旧组，再统一置 `interrupted` |
| 启动登记窗口强杀不留孤儿 | 两段式登记：启动令牌先落库再 spawn，进程环境携带令牌；恢复时扫描 `/proc/*/environ` 凭令牌找回并清理 |
| 身份登记失败回收进程 | `set_step_process` 写库失败时立即终止已产生的进程组，步骤判 `failed` |
| 恢复途中再重启仍能清理 | 恢复幂等可重入：清理在标记中断之前完成，途中被强杀则下次启动重扫重清 |
| 未跑步骤不再启动、不误杀后来构建 | 恢复不恢复调度；身份不符（PID 复用）或标记对不上的进程绝不触碰 |
| 重复取消不改终态 | 终态构建上的取消请求幂等忽略，返回说明，不写库 |
| 逐步状态与日志 | `GET /builds/{id}` 全量状态；`GET /builds/{id}/steps/{sid}/logs`（支持 `?tail=N`） |

步骤状态机：`pending → starting → running → cleaning_up → succeeded|failed|cancelled|interrupted`，
失败后代为 `skipped`。构建终态：全部成功 `succeeded`；有失败 `failed`；
取消导致的终止 `cancelled`；重启中断 `interrupted`。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# API:      http://localhost:8000
# Swagger:  http://localhost:8000/docs
# 健康检查: http://localhost:8000/health
```

容器以 `tini` 作为 PID 1 回收孤儿/僵尸进程；数据全部落在命名卷 `pipeline-data`
（SQLite、工作目录、步骤日志），重启不丢、恢复逻辑依赖它。

## 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
PIPELINE_DATA_DIR=./data .venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 示例构建

`examples/` 下提供三类（另含超时示例）：

- `success_build.json` — prepare → compile → unit-test → package 全成功链路
- `failure_build.json` — 编译失败、后代跳过、独立分支继续
- `slow_child_build.json` — 主命令退出 0 但派生 `sleep 300` 慢子进程，验证清理与判失败
- `timeout_build.json` — 2s 超时终止睡眠 60s 的进程组

一键演示（API 已启动时）：

```bash
.venv/bin/python examples/run_demo.py            # 默认 http://127.0.0.1:8000
```

手动提交：

```bash
curl -s -X POST localhost:8000/builds \
  -H 'Content-Type: application/json' \
  --data @examples/success_build.json
curl -s localhost:8000/builds/1
curl -s localhost:8000/builds/1/steps/compile/logs
curl -s -X POST localhost:8000/builds/1/cancel
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/builds` | 提交 DAG 构建（201 返回含逐步状态） |
| GET | `/builds?limit=N` | 构建列表（新→旧） |
| GET | `/builds/{id}` | 构建与全部步骤状态 |
| POST | `/builds/{id}/cancel` | 整次取消（幂等） |
| GET | `/builds/{id}/steps/{step_id}/logs?tail=N` | 步骤日志 |
| GET | `/health` | 健康检查与最近一次启动恢复报告 |
| GET | `/docs` | Swagger UI |

## 进程组与重启恢复的关键设计

1. 每个步骤由 `/bin/sh -c <command>` 在新会话中启动，主命令为进程组组长；
   命令派生出的所有子进程默认同组，可被 `killpg` 一次性覆盖。
2. 步骤终态确认前必须满足：`/proc` 扫描该 PGID 已无任何非僵尸成员。
   清理序列为 SIGTERM 宽限（默认 5s）→ SIGKILL → 硬等排空（默认 60s 有界）。
3. 主命令退出码为 0 但组内仍有成员时，清理后步骤判 `failed`，杜绝"误发包"。
4. **启动登记两段式**（1.1.0 修复的强杀窗口）：先把本次启动令牌
   （uuid）写入 `steps.start_token` 并把步骤置为 `starting`，再 spawn
   （进程环境携带 `PIPELINE_STEP_TOKEN` 及 `PIPELINE_BUILD_ID/PIPELINE_STEP_ID`），
   最后才登记 `(pid, pgid, proc_starttime)` 并置 `running`。令牌落库前不存在
   进程；令牌落库后任何时刻被强杀，恢复都能凭环境标记找回已产生的进程。
   身份登记写库失败时，已产生的进程组会被立即回收，步骤判 `failed`。
5. 重启恢复对每个非终态步骤走两条互相补盲的识别路径：
   - **身份校验**：`pid + /proc starttime + pgid` 三者一致才动组，PID 复用的
     后来进程不会被误杀；
   - **环境标记扫描**：扫描 `/proc/*/environ`，按启动令牌（旧数据卷没有令牌，
     回退到 `PIPELINE_BUILD_ID + PIPELINE_STEP_ID` 环境对）认出自家进程，
     覆盖"已启动未登记"窗口与身份失效（组长已死）两种情形。
   两条路径都只匹配本步骤派生的进程，无关进程绝不触碰。
6. 恢复在应用开始接受请求前完成：所有残留进程组清空前，不标记 `interrupted`、
   不放行新构建。恢复幂等可重入——恢复途中再次被强杀，下次启动重新扫描
   仍能完成清理。旧构建统一置 `interrupted`，调度不恢复，未跑步骤不补跑。
7. 旧版本数据卷无缝兼容：启动时自动 `ALTER TABLE steps ADD COLUMN start_token`，
   旧行令牌为 NULL 时恢复扫描回退到环境对匹配。

已知残余风险（Unix 进程模型的固有限制）：步骤命令若自行 `setsid` 脱离会话
**并且**清洗环境变量，其后代将无法被任何标记识别；这与所有 Unix 进程
监管方案一致。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

- `tests/test_validation.py`：DAG 校验、12 步上限、超时正数、进程身份识别。
- `tests/test_proc.py`：进程组 ID 读取、环境标记扫描（只认自家进程）、
  顽固残留进程的 TERM→KILL 清理。
- `tests/test_recovery.py`：旧数据卷就地迁移、身份登记失败回收已产生进程、
  登记窗口期孤儿的恢复清理（含未跑步骤不补跑）、终态/陌生/无标记进程不误杀。
- `tests/test_integration.py`：以真实 uvicorn 子进程端到端验证成功、失败跳过、
  慢子进程清理判失败、超时、并行上限 2、整次取消、重复取消幂等、慢清理不占
  另一名额、重启恢复（含不误杀后来构建）、**登记窗口期 SIGKILL 后孤儿清理与
  工作目录停写**、**恢复途中再次 SIGKILL 仍能完成清理**、错误请求与文档接口。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PIPELINE_DATA_DIR` | `./data` | SQLite/工作目录/日志根目录 |
| `PIPELINE_MAX_STEPS` | `12` | 单构建步骤上限 |
| `PIPELINE_MAX_PARALLEL` | `2` | 全局并行名额 |
| `PIPELINE_TIMEOUT_CAP_SECONDS` | `86400` | 单步超时上限 |
| `PIPELINE_TERM_GRACE_SECONDS` | `5` | SIGTERM 宽限 |
| `PIPELINE_KILL_GRACE_SECONDS` | `60` | SIGKILL 后硬等上限 |
| `PIPELINE_SPAWN_REGISTER_DELAY_SECONDS` | `0` | 故障注入：放大"已启动未登记"窗口，仅供测试，生产必须为 0 |
