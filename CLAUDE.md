# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概览

DMM-Agent 是 DMM-CM 系统的本地客户端，单文件 Python WebSocket 客户端，负责本地视频文件扫描与整理。配合 [DMM-CM](https://github.com/your-org/DMM-CM) cloud 后端使用。

## 核心约束

- **单文件** — `agent.py` 必须保持单文件，所有逻辑集中在一个文件中
- **最小依赖** — 仅 stdlib + `websockets`，无其他外部依赖
- **跨平台** — 支持 Windows、macOS、Linux

## 配置系统

- `config.json`（已 gitignore）覆盖内置默认值
- 未指定的字段使用 `agent.py` 中的 `DEFAULT_CONFIG`
- 绑定成功后 `DEVICE_TOKEN` 自动写入 `config.json`
- 开发模式通过 `"DEV": true` 启用（自动 dev-bind，无需 Telegram）

## 常用命令

```bash
# 安装依赖
pip install websockets

# 启动
python3 agent.py

# Windows 一键启动（自动创建 venv）
.\run.ps1
```

## 代码结构

`agent.py` 内部按功能分区：

1. **配置加载** — `DEFAULT_CONFIG` + `config.json` 合并
2. **WebSocket 连接** — 认证、心跳、重连
3. **文件扫描** — 遍历 `BASE_DIRS`，提取番号
4. **任务处理** — SCAN / MOVE / OPEN_FILE
5. **文件操作** — 移动、重命名、跨盘 copy+delete

## WebSocket 协议

与 DMM-CM cloud 的 `/ws/agent` 端点通信：

- 认证：`AUTH` 消息 + `DEVICE_TOKEN`
- 心跳：30s 间隔
- 限流：10 msg/s、1 MB 消息上限
- 消息类型：`SYNC_REPORT`、`TASK_RESULT`、`TASK_PROGRESS`

## 测试

```bash
python3 -m pytest test_agent.py -v  # 如果存在测试文件
```
