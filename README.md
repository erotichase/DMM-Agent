# DMM-Agent

本地视频管理助手，单文件部署，零外部依赖（仅需 Python 3.10+ 和 websockets）。

## 快速开始

### 1. 创建配置文件

复制示例配置并编辑：

```bash
cp config.example.json config.json
```

编辑 `config.json`：

```json
{
  "CLOUD_WS_URL": "wss://dmmcm.duckdns.org/ws/agent",
  "DEVICE_TOKEN": "",
  "BASE_DIRS": ["/path/to/your/videos"],
  "TARGET_DIRS": ["/path/to/organized"],
  "FFPROBE_PATH": ""
}
```

### 2. 启动

**Linux/macOS：**

```bash
pip install websockets
python3 agent.py
```

**Windows：**

```powershell
.\run.ps1
```

`run.ps1` 会自动创建 venv、安装依赖并启动 agent。

### 3. 首次绑定

首次启动时 `DEVICE_TOKEN` 为空，Agent 会打印 Telegram Bot 链接，点击链接完成绑定：

```
========================================================
  DMM-CM Agent — Device Binding
========================================================

  Please click the link below to bind this device:
  https://t.me/YourBot?start=bind_ABC123

  This link expires in 5 minutes.
========================================================
```

绑定成功后 token 自动写入 `config.json`，后续启动无需重复绑定。

## 配置说明

所有配置通过 `config.json` 管理（已加入 .gitignore，代码更新不会覆盖）。

| 字段           | 必填 | 说明                                                   |
| -------------- | ---- | ------------------------------------------------------ |
| `CLOUD_WS_URL` | 是   | 云端 WebSocket 地址                                    |
| `DEVICE_TOKEN` | 否   | 设备令牌（首次启动自动获取）                           |
| `BASE_DIRS`    | 是   | 视频扫描目录列表，绝对路径                             |
| `TARGET_DIRS`  | 是   | 整理目标目录列表，优先使用第一个                       |
| `FFPROBE_PATH` | 否   | ffprobe 路径（Windows 必填，Linux/macOS 留空自动检测） |

**配置示例：**

```json
// Windows
{
    "CLOUD_WS_URL": "wss://dmmcm.duckdns.org/ws/agent",
    "BASE_DIRS": ["F:/JAVTMP"],
    "TARGET_DIRS": ["F:/JAV"],
    "FFPROBE_PATH": "F:/JAVTool/ffprobe.exe"
}

// macOS
{
    "BASE_DIRS": ["/Users/chase/Downloads/115"],
    "TARGET_DIRS": ["/Users/chase/Downloads/Collection"]
}

// Linux NAS
{
    "CLOUD_WS_URL": "wss://dmmcm.duckdns.org/ws/agent",
    "BASE_DIRS": ["/mnt/disk1", "/mnt/disk2"],
    "TARGET_DIRS": ["/mnt/storage"]
}
```

> 只需写你要修改的字段，未指定的使用内置默认值。

## 启动日志

```
============================================================
DMM-CM Agent v1.0.0
配置: /path/to/config.json
云端: wss://dmmcm.duckdns.org/ws/agent
扫描: F:/JAVTMP
ffprobe: ✓ (F:/JAVTool/ffprobe.exe)
============================================================
正在连接 wss://dmmcm.duckdns.org/ws/agent ...
✓ 已连接 (设备 ID: 1)
扫描完成: 26 个番号
```

## 自动整理

Agent 连接成功后会自动整理文件到标准目录结构。

整理模板（agent.py 内配置）：

```python
ORGANIZE_PATTERN = "{actress}/{code}"  # 可用变量: {actress}, {code}, {series}
ORGANIZE_ON_CONFLICT = "skip"          # skip / rename / overwrite
```

整理前后示例：

```
F:/JAVTMP/JUR-582.mp4
  ↓
F:/JAV/桜空もも/JUR-582/JUR-582.mp4
```

- 跨盘移动自动降级为 copy+delete
- 传输过程中创建 `.dmm-tmp` 临时文件
- 仅在初始连接后执行一次

## 支持的任务类型

| 任务      | 说明                                                 |
| --------- | ---------------------------------------------------- |
| SCAN      | 扫描 BASE_DIRS 下的视频文件，提取番号上报云端        |
| MOVE      | 按元数据将视频移动到标准目录结构                     |
| ORGANIZE  | 按 `ORGANIZE_PATTERN` 模板自动整理文件到 TARGET_DIRS |
| OPEN_FILE | 打开指定视频文件（远程播放，不经过任务队列直接处理） |

## 常见问题

### 连接失败

```
[ERROR] 连接断开: [Errno 61] Connection refused
```

检查 `CLOUD_WS_URL` 是否正确。

### 认证失败

```
[ERROR] 鉴权失败: invalid_token
```

编辑 `config.json`，清空 `DEVICE_TOKEN`，重启 Agent 重新绑定。

### ffprobe 未检测到

```
[WARNING] ffprobe: ✗ (未检测到，视频元数据将不可用。FFPROBE_PATH='')
```

在 `config.json` 中设置 `FFPROBE_PATH` 为 ffprobe 的完整路径。

### 多个 Agent 冲突

```
[ERROR] Agent 已在运行中（锁文件 /tmp/dmm-agent.lock）
```

同一台机器只能运行一个实例。如需多个：

```bash
export DMM_AGENT_LOCK=/tmp/dmm-agent-2.lock
python3 agent.py
```

## 后台运行

```bash
# Linux/macOS
nohup python3 agent.py > agent.log 2>&1 &

# systemd
sudo systemctl enable dmm-agent && sudo systemctl start dmm-agent

# Windows — 使用 run.ps1 或创建计划任务
```

## 安全提示

1. **不要分享 DEVICE_TOKEN** — 它相当于设备密码
2. **定期备份 config.json** — 避免重新绑定
3. **避免 root 运行** — Agent 会警告 root 权限风险
