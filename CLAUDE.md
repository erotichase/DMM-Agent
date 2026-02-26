# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概览

DMM-Agent 是 DMM-CM 系统的本地客户端，单文件 Python WebSocket 客户端（~1500 行），负责本地视频文件扫描、元数据提取、与云端同步，并执行云端下发的文件操作任务。配合 DMM-CM cloud 后端使用。

## 核心约束

- **单文件** — 所有逻辑必须在 `agent.py` 一个文件中，禁止拆分模块
- **最小依赖** — 仅 stdlib + `websockets>=12.0`，禁止引入其他外部依赖
- **跨平台** — Windows/macOS/Linux 均需支持（注意路径分隔符、锁文件、信号处理的平台差异）

## 常用命令

```bash
pip install websockets          # 安装依赖
python3 agent.py                # 启动 agent
python3 -m pytest test_agent.py -v  # 运行测试（如存在）
```

## 架构：ws_session 生命周期

`agent.py` 的核心是 `ws_session()` 协程，按阶段执行：

1. **Phase 1: AUTH** — 发送 HMAC 签名鉴权消息（`build_auth_payload()`），等待 `AUTH_OK`。永久失败（invalid_token 等）抛 `TokenInvalidError` 触发重新绑定
2. **Phase 2: SCAN + SYNC** — 调用 `scan_local_files()` 扫描 `BASE_DIRS`，通过 `build_sync_report()` 构建全量/增量/分片报告，发送 `SYNC_REPORT` 并等待 `SYNC_ACK`。等待期间收到的其他消息缓存到 `_buffered_messages`
3. **Phase 3: 并发消息循环** — 三个 asyncio task 并行：
   - `heartbeat_loop`: 30s±5s 发送 PING
   - `task_worker`: 单 worker 串行执行任务（文件操作需要串行化），通过 `run_in_executor` 在线程池执行，边执行边发 `TASK_PROGRESS`
   - `message_loop`: 处理所有入站消息（EXEC_TASK → 入队，SYNC_ACK/REJECT，TOKEN_ROTATE，OPEN_FILE，DEVICE_DELETED 等）

外层 `run_forever()` 负责断线重连（指数退避 + 抖动），令牌失效时自动进入绑定流程。

## WebSocket 协议消息类型

**Agent → Cloud**: AUTH, SYNC_REPORT, TASK_RESULT, TASK_PROGRESS, PING, GOODBYE, TOKEN_ROTATE_ACK
**Cloud → Agent**: AUTH_OK/AUTH_ACK, SYNC_ACK, SYNC_REJECT, SYNC_SHARD_ACK, EXEC_TASK, TASK_CANCEL, PONG, TOKEN_ROTATE, OPEN_FILE, DEVICE_DELETED

## 任务系统

`execute_task()` 分发三类动作：

- **MOVE** — `_execute_move()`: 文件移动/重命名，跨盘自动 copy+delete，`.dmm-tmp` 临时文件防中断，完成后清理空目录
- **SCAN** — `_execute_scan()`: 重新扫描文件
- **ORGANIZE** — `_execute_organize()`: 按 `ORGANIZE_PATTERN`（默认 `{actress}/{code}`）自动整理，成功后触发全量重新同步

所有任务 5 分钟超时。

## 配置系统

`config.json`（gitignore）覆盖 `_DEFAULT_CONFIG` 内置默认值。绑定成功后 `DEVICE_TOKEN` 自动通过 `_persist_token()` 写回 config.json。

开发模式 `"DEV": true`：自动调用 `/api/v1/auth/dev-bind` 绑定，websockets v14+ 启用 proxy 支持。

## 关键模式

- **单实例锁** — `acquire_singleton_lock()` 通过文件锁（Windows: msvcrt，Unix: fcntl）防止多进程
- **番号提取** — `CODE_PATTERN` 正则从文件名提取番号，`normalize_code()` 统一为大写带横杠格式
- **增量同步** — 基于 `_last_sync_version` 和 `_last_synced_codes` 计算 diff，SYNC_REJECT 时回退全量
- **哨兵文件** — `ensure_sentinel()` 在每个 BASE_DIR 创建 `.dmm-agent` 文件作为目录指纹
- **路径安全** — `is_path_safe()` 校验路径遍历，`sanitize_error()` 清理错误信息中的敏感路径
