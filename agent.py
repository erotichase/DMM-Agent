#!/usr/bin/env python3
"""DMM-CM Local Agent

本地 Agent 单文件部署，核心职责：
1. WebSocket 连接云端，HMAC 鉴权
2. 扫描本地视频文件，增量同步至云端
3. 执行云端下发的文件操作任务（移动/重命名/扫描）
4. 心跳保活，断线自动重连

依赖：Python 3.10+, websockets
配置：在 agent 目录下创建 config.json（参考 config.example.json）
"""

# ===== 用户配置区 =====

import os
import json as _json_mod
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_FILE = os.path.join(_SCRIPT_DIR, "config.json")

# 默认配置（config.json 中的同名字段会覆盖）
_PROD_WS_URL = "wss://dmmcm.duckdns.org/ws/agent"
_DEV_WS_URL = "ws://127.0.0.1:8000/ws/agent"

_DEFAULT_CONFIG = {
    "DEVICE_TOKEN": "",
    "BASE_DIRS": [],
    "TARGET_DIRS": [],
    "FFPROBE_PATH": "",
    "DEV": False,
}

# 加载配置：config.json > 默认值
if os.path.exists(_CONFIG_FILE):
    with open(_CONFIG_FILE, "r", encoding="utf-8") as f:
        _config = {**_DEFAULT_CONFIG, **_json_mod.load(f)}
else:
    _config = dict(_DEFAULT_CONFIG)

IS_DEV = _config.get("DEV", False)
DEV_TG_ID = _config.get("DEV_TG_ID", 111111111)
DEV_USER_NAME = _config.get("DEV_USER_NAME", "Dev User A")
CLOUD_WS_URL = _config.get("CLOUD_WS_URL", _DEV_WS_URL if IS_DEV else _PROD_WS_URL)
DEVICE_TOKEN = _config.get("DEVICE_TOKEN", "")
BASE_DIRS = _config.get("BASE_DIRS", [])
TARGET_DIRS = _config.get("TARGET_DIRS", [])
FFPROBE_PATH = _config.get("FFPROBE_PATH", "")

# 通用配置
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".wmv"}  # 支持的视频文件扩展名
HEARTBEAT_INTERVAL = 30  # 心跳间隔（秒）

# 自动整理配置
ORGANIZE_PATTERN = "{actress}/{code}"  # 整理目录模板，可用变量: {actress} 女优名, {code} 番号, {series} 系列名
ORGANIZE_ON_CONFLICT = "skip"  # 文件冲突处理策略: skip=跳过保留原文件, rename=重命名新文件, overwrite=覆盖（仅当新文件更大时）

# ===== 配置区结束 =====

import asyncio
import hashlib
import hmac as hmac_mod
import json
import logging
import platform
import queue
import random
import re
import shutil
import signal
import socket
import sys
import time
import unicodedata
import uuid
import errno as errno_mod
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("agent")

# 内部常量
VERSION = "1.0.0"
_default_lock = os.path.join(os.environ.get("TEMP", "/tmp"), "dmm-agent.lock")
LOCK_FILE = Path(os.environ.get("DMM_AGENT_LOCK", _default_lock))  # 单实例锁文件路径
RECONNECT_BASE = 5  # 重连基础延迟（秒）
RECONNECT_MAX = 60  # 重连最大延迟（秒）
RECONNECT_JITTER = 5  # 重连抖动范围（秒）
_PERMANENT_AUTH_REASONS = {"invalid_token", "token_revoked", "device_deleted"}  # 永久鉴权失败原因（需重新绑定）


class TokenInvalidError(Exception):
    """设备令牌永久失效异常（需重新绑定设备）"""

# 番号提取正则
CODE_PATTERN = re.compile(r"^([A-Za-z]{2,10})-?(\d{2,6})")  # 匹配番号主体
CD_SUFFIX_PATTERN = re.compile(r"-CD(\d+)$", re.IGNORECASE)  # 匹配 CD 后缀

# 全局状态
_shutdown_event = None  # 优雅退出事件
_ws_connection = None  # 当前 WebSocket 连接
_last_sync_version: int = 0  # 增量同步版本号
_last_synced_codes: set[str] = set()  # 上次同步的番号集合
_my_organized_codes: set[str] = set()  # 本会话 ORGANIZE 过的番号

# ═══════════════════════════════════════════════════════════════
# 扫描缓存（跨会话持久化，避免重启时全量重扫）
# ═══════════════════════════════════════════════════════════════

_SCAN_CACHE_FILE = os.path.join(_SCRIPT_DIR, ".scan_cache.json")
_SCAN_CACHE_FLUSH_STEP = 50  # 每扫描 N 个新番号刷一次磁盘


def _load_scan_cache() -> dict | None:
    """加载扫描缓存，返回 None 表示无缓存或缓存无效"""
    try:
        with open(_SCAN_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = _json_mod.load(f)
        # 校验：BASE_DIRS 必须一致（目录变了缓存无效）
        if cache.get("base_dirs") != [os.path.normcase(os.path.abspath(d)) for d in BASE_DIRS]:
            logger.info("缓存失效: BASE_DIRS 已变更")
            return None
        if not cache.get("files"):
            return None
        return cache
    except (OSError, _json_mod.JSONDecodeError, KeyError):
        return None


def _save_scan_cache(files: list[dict], sync_version: int = 0,
                     complete: bool = True) -> None:
    """保存扫描缓存到磁盘"""
    cache = {
        "base_dirs": [os.path.normcase(os.path.abspath(d)) for d in BASE_DIRS],
        "files": files,
        "sync_version": sync_version,
        "complete": complete,
        "cached_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    tmp = _SCAN_CACHE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            _json_mod.dump(cache, f, ensure_ascii=False)
        # 原子替换
        if sys.platform == "win32":
            # Windows: os.replace 是原子的
            os.replace(tmp, _SCAN_CACHE_FILE)
        else:
            os.rename(tmp, _SCAN_CACHE_FILE)
    except OSError as e:
        logger.debug("缓存写入失败: %s", e)
        try:
            os.unlink(tmp)
        except OSError:
            pass


def _invalidate_scan_cache() -> None:
    """删除扫描缓存（SYNC_REJECT 或数据不一致时调用）"""
    try:
        os.unlink(_SCAN_CACHE_FILE)
    except OSError:
        pass

def _detect_ffprobe() -> str:
    """检测 ffprobe 路径（优先配置路径，其次系统 PATH），返回可执行路径或空串。

    检测成功后执行 `ffprobe -version` 验证可执行性。
    """
    import subprocess

    path = ""
    if FFPROBE_PATH:
        if os.path.isfile(FFPROBE_PATH):
            path = FFPROBE_PATH
        else:
            logger.warning("FFPROBE_PATH 指定的文件不存在: %s", FFPROBE_PATH)
            return ""
    else:
        path = shutil.which("ffprobe") or ""

    if not path:
        return ""

    # 验证可执行性
    try:
        result = subprocess.run(
            [path, "-version"],
            capture_output=True, timeout=10,
        )
        if result.returncode != 0:
            logger.warning("ffprobe 存在但无法执行: %s (returncode=%d)", path, result.returncode)
            return ""
        version_line = result.stdout.decode("utf-8", errors="replace").split("\n")[0]
        logger.info("ffprobe 版本: %s", version_line.strip())
    except Exception as exc:
        logger.warning("ffprobe 存在但执行失败: %s (%s)", path, exc)
        return ""

    return path

_FFPROBE_CMD = _detect_ffprobe()
_HAS_FFPROBE = bool(_FFPROBE_CMD)


# ═══════════════════════════════════════════════════════════════
# 单实例锁
# ═══════════════════════════════════════════════════════════════

def acquire_singleton_lock() -> int:
    """获取单实例文件锁，防止多进程运行（内核自动释放，无需手动清理）"""
    fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, IOError):
        logger.error("Agent 已在运行中（锁文件 %s）。请勿启动多个实例。", LOCK_FILE)
        sys.exit(1)
    # Write PID for run.sh / run.ps1 to kill
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, f"{os.getpid()}\n".encode())
    return fd


# ═══════════════════════════════════════════════════════════════
# 番号工具
# ═══════════════════════════════════════════════════════════════

def normalize_code(raw: str) -> str:
    """标准化番号格式（ABC123 → ABC-123，abc-123 → ABC-123）"""
    if not raw:
        return ""
    raw = raw.strip().upper()

    # 已含连字符
    if "-" in raw:
        parts = raw.split("-", 1)
        prefix = re.sub(r"^[\d_]+", "", parts[0])
        return f"{prefix}-{parts[1]}" if prefix else raw

    # 无连字符: 找字母和数字的分界点
    match = re.match(r"^([A-Z_]+?)(\d+.*)$", raw)
    if match:
        prefix = match.group(1).rstrip("_")
        if prefix:
            return f"{prefix}-{match.group(2)}"

    return raw


def extract_code_from_filename(filename: str) -> str:
    """从文件名提取并标准化番号"""
    stem = Path(filename).stem
    stem = CD_SUFFIX_PATTERN.sub("", stem)
    match = CODE_PATTERN.match(stem)
    if not match:
        return ""
    return normalize_code(match.group(0))


def extract_cd_number(stem: str) -> int:
    """提取 CD 编号"""
    match = CD_SUFFIX_PATTERN.search(stem)
    return int(match.group(1)) if match else 0


# ═══════════════════════════════════════════════════════════════
# 目录哨兵文件
# ═══════════════════════════════════════════════════════════════

SENTINEL_NAME = ".dmm-agent-marker"


def ensure_sentinel(base_dir: str) -> str:
    """确保目录存在哨兵文件，返回唯一指纹（防误操作）"""
    sentinel_path = Path(base_dir) / SENTINEL_NAME
    if sentinel_path.exists():
        try:
            data = json.loads(sentinel_path.read_text(encoding="utf-8"))
            return data.get("fingerprint", "")
        except (json.JSONDecodeError, OSError):
            pass

    fingerprint = str(uuid.uuid4())
    data = {
        "fingerprint": fingerprint,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    try:
        sentinel_path.write_text(json.dumps(data), encoding="utf-8")
        os.chmod(str(sentinel_path), 0o644)
    except OSError as e:
        logger.warning("无法写入哨兵文件 %s: %s", sentinel_path, e)
    return fingerprint


def update_sentinel_device_id(device_id: int):
    """鉴权成功后更新所有哨兵文件的设备 ID"""
    for base_dir in BASE_DIRS:
        sentinel_path = Path(base_dir) / SENTINEL_NAME
        if not sentinel_path.exists():
            continue
        try:
            data = json.loads(sentinel_path.read_text(encoding="utf-8"))
            if data.get("device_id") == device_id:
                continue
            data["device_id"] = device_id
            sentinel_path.write_text(json.dumps(data), encoding="utf-8")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("无法更新哨兵 device_id %s: %s", sentinel_path, e)


# ═══════════════════════════════════════════════════════════════
# 视频元数据探测
# ═══════════════════════════════════════════════════════════════

_ffprobe_fail_count = 0  # 累计失败次数（前 5 次打日志，之后每 50 次打一次）


def probe_video_metadata(filepath: Path) -> dict | None:
    """探测视频元数据（分辨率/编码/码率/音频），失败返回 None"""
    global _ffprobe_fail_count
    if not _HAS_FFPROBE:
        return None
    import subprocess
    try:
        # cwd=父目录 + 只传文件名，避免 ffprobe 处理长路径或特殊字符
        result = subprocess.run(
            [_FFPROBE_CMD, "-v", "error", "-print_format", "json",
             "-show_streams", filepath.name],
            capture_output=True, timeout=15,
            cwd=str(filepath.parent),
        )
        if result.returncode != 0:
            _ffprobe_fail_count += 1
            if _ffprobe_fail_count <= 5 or _ffprobe_fail_count % 50 == 0:
                stderr = result.stderr.decode("utf-8", errors="replace")[:200]
                logger.warning("ffprobe 返回非零: code=%d, file=%s, stderr=%s (fail #%d)",
                               result.returncode, filepath, stderr, _ffprobe_fail_count)
            return None
        data = json.loads(result.stdout.decode("utf-8", errors="replace"))
        video_stream = None
        audio_codec = None
        for s in data.get("streams", []):
            if s.get("codec_type") == "video" and video_stream is None:
                video_stream = s
            elif s.get("codec_type") == "audio" and audio_codec is None:
                audio_codec = s.get("codec_name", "")
        if not video_stream:
            return None
        width = int(video_stream.get("width", 0))
        height = int(video_stream.get("height", 0))
        return {
            "width": width,
            "height": height,
            "codec": video_stream.get("codec_name", ""),
            "bitrate": video_stream.get("bit_rate", ""),
            "audio": audio_codec or "",
        }
    except Exception as exc:
        _ffprobe_fail_count += 1
        if _ffprobe_fail_count <= 5 or _ffprobe_fail_count % 50 == 0:
            logger.warning("ffprobe 执行异常: %s (file=%s, fail #%d)", exc, filepath, _ffprobe_fail_count)
        return None


def resolution_from_height(height: int) -> str:
    """高度映射分辨率标签（2160→4K, 1080→1080P, 720→720P）"""
    if height >= 2160:
        return "4K"
    if height >= 1080:
        return "1080P"
    if height >= 720:
        return "720P"
    return "SD"


_ILLEGAL_DIR_CHARS = re.compile(r'[/\\:*?"<>|\x00]')


def sanitize_dirname(name: str) -> str:
    r"""清理目录名非法字符（/ \ : * ? " < > |）"""
    return _ILLEGAL_DIR_CHARS.sub('_', name).strip()


# ═══════════════════════════════════════════════════════════════
# 本地文件扫描
# ═══════════════════════════════════════════════════════════════

def _list_videos_win(base_dir: str) -> list[str] | None:
    """Windows: 使用 dir /s /b 快速递归列出视频文件，返回绝对路径列表。

    失败返回 None（由调用方降级到 rglob）。空列表 [] 表示无文件。
    """
    import subprocess

    # 构建 dir /s /b 命令，每个扩展名一个通配符
    patterns = " ".join(f'"{base_dir}\\*.{ext.lstrip(".")}"' for ext in VIDEO_EXTENSIONS)
    # cmd /U: dir 输出 UTF-16LE（对管道/重定向生效），正确处理 Unicode 文件名
    # 用字符串传参，避免 list2cmdline 对内部引号二次转义
    try:
        result = subprocess.run(
            f"cmd /U /c dir /s /b {patterns} 2>nul",
            capture_output=True, timeout=120,
        )
        # dir /s /b 找不到文件时 returncode=1，stdout 为空
        raw = result.stdout
        if not raw:
            return []
        output = raw.decode("utf-16-le", errors="replace").strip()
        if not output:
            return []
        return [line for line in output.splitlines() if line.strip()]
    except Exception as exc:
        logger.warning("dir /s /b 执行失败，降级到 rglob: %s", exc)
        return None


def scan_local_files(include_target: bool = True, skip_probe: bool = False,
                     on_progress: "Callable[[int], None] | None" = None,
                     flush_cache: bool = False) -> list[dict]:
    """扫描视频文件，返回 [{code, paths, size, res?, meta?}]

    Args:
        include_target: True=扫描 BASE_DIRS + TARGET_DIRS（sync 用），
                        False=仅扫描 BASE_DIRS（scan/organize 用）
        skip_probe: True=跳过 ffprobe 探测（SCAN/ORGANIZE 用，加速返回）
        on_progress: 可选回调，每发现一个新番号时调用 on_progress(current_code_count)
        flush_cache: True=扫描过程中增量写入缓存（仅 pre-scan 初始同步使用）
    """
    code_files: dict[str, list[tuple[int, str, int, Path]]] = {}
    _flush_counter = 0  # 距上次刷盘新增的 code 计数

    def _maybe_flush_cache():
        """达到步长时将当前结果写入缓存（incomplete 标记）"""
        nonlocal _flush_counter
        if not flush_cache:
            return
        _flush_counter += 1
        if _flush_counter >= _SCAN_CACHE_FLUSH_STEP:
            _flush_counter = 0
            partial = _build_results_from(code_files, {})
            _save_scan_cache(partial, sync_version=0, complete=False)

    # 构建扫描目录列表（去重，防止子目录重叠导致重复扫描）
    dirs_to_scan = list(BASE_DIRS)
    if include_target:
        dirs_to_scan = dirs_to_scan + list(TARGET_DIRS)
    all_dirs: list[str] = []
    seen: set[str] = set()
    for d in dirs_to_scan:
        normalized = os.path.normcase(os.path.abspath(d))
        if normalized not in seen:
            seen.add(normalized)
            all_dirs.append(d)

    for base_dir in all_dirs:
        root = Path(base_dir)
        if not root.is_dir():
            logger.warning("扫描目录不存在: %s", base_dir)
            continue

        # Windows: dir /s /b 快速列表，失败降级到 rglob
        video_paths = None
        if sys.platform == "win32":
            video_paths = _list_videos_win(base_dir)

        if video_paths is not None:
            iter_paths = (Path(p) for p in video_paths)
        else:
            iter_paths = (p for p in root.rglob("*")
                          if p.is_file() and p.suffix.lower() in VIDEO_EXTENSIONS)

        for path in iter_paths:
            if path.name == SENTINEL_NAME or path.name.endswith(".dmm-tmp"):
                continue

            code = extract_code_from_filename(path.name)
            if not code:
                continue

            cd_num = extract_cd_number(path.stem)
            # 计算相对路径并 NFC 归一化，统一使用 / 分隔符（跨平台一致）
            rel_path = path.relative_to(root).as_posix()
            rel_path = unicodedata.normalize("NFC", rel_path)

            try:
                file_size = path.stat().st_size
            except OSError:
                file_size = 0

            is_new_code = code not in code_files
            if is_new_code:
                code_files[code] = []
            code_files[code].append((cd_num, rel_path, file_size, path))
            if is_new_code:
                if on_progress is not None:
                    on_progress(len(code_files))
                _maybe_flush_cache()

    # ffprobe 后置并发探测（skip_probe=True 时跳过，SCAN/ORGANIZE 用）
    code_meta: dict[str, dict] = {}
    if not skip_probe and _HAS_FFPROBE:
        # 每个 code 取第一个文件的绝对路径
        to_probe: list[tuple[str, Path]] = []
        for code, file_list in code_files.items():
            to_probe.append((code, file_list[0][3]))

        # 按路径排序减少 HDD 磁头随机寻道
        to_probe.sort(key=lambda x: str(x[1]))

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(probe_video_metadata, p): c for c, p in to_probe}
            for future in as_completed(futures):
                c = futures[future]
                try:
                    meta = future.result()
                    if meta:
                        code_meta[c] = meta
                except Exception:
                    pass

    results = _build_results_from(code_files, code_meta)

    # 扫描完成：写入完整缓存
    if flush_cache:
        _save_scan_cache(results, sync_version=0, complete=True)

    return results


def _build_results_from(code_files: dict, code_meta: dict) -> list[dict]:
    """从 code_files 字典构建结果列表（scan_local_files 和缓存恢复共用）"""
    results = []
    for code, file_list in code_files.items():
        file_list.sort(key=lambda x: x[0])
        total_size = sum(f[2] for f in file_list)
        paths = [f[1] for f in file_list]

        first_meta = code_meta.get(code)
        res = None
        meta_out = None
        if first_meta:
            height = first_meta.get("height", 0)
            if height > 0:
                res = resolution_from_height(height)
            meta_out = {k: v for k, v in first_meta.items() if v}

        entry: dict = {"code": code, "paths": paths, "size": total_size}
        if res:
            entry["res"] = res
        if meta_out:
            entry["meta"] = meta_out
        results.append(entry)

    return results


def _get_my_files(on_progress: "Callable[[int], None] | None" = None) -> list[dict]:
    """返回属于当前用户的文件列表

    BASE_DIRS 文件 + 本会话 ORGANIZE 过的文件（从 TARGET_DIRS 捞回）。
    初始 SYNC 只上报 BASE_DIRS，ORGANIZE 后按需补报。
    """
    base_files = scan_local_files(include_target=False, on_progress=on_progress)
    if not _my_organized_codes:
        return base_files

    all_files = scan_local_files(include_target=True)
    base_codes = {f["code"] for f in base_files}
    code_to_file = {f["code"]: f for f in base_files}
    for f in all_files:
        if f["code"] in _my_organized_codes and f["code"] not in base_codes:
            code_to_file[f["code"]] = f
    return list(code_to_file.values())


def build_sync_report(incremental: bool = True, prefetched_files: list[dict] | None = None) -> dict | list[dict]:
    """构建同步报告（增量 diff 或全量分片）

    Args:
        incremental: True=尝试增量 diff，False=强制全量
        prefetched_files: 预先获取的文件列表（跳过 _get_my_files 扫描）
    """
    global _last_synced_codes

    files = prefetched_files if prefetched_files is not None else _get_my_files()
    current_codes = {f["code"] for f in files}

    # 目录指纹
    dir_fingerprints = {}
    storage = []
    for idx, base_dir in enumerate(BASE_DIRS):
        dir_fingerprints[str(idx)] = ensure_sentinel(base_dir)
        try:
            usage = shutil.disk_usage(base_dir)
            storage.append({
                "dir_index": idx,
                "free_bytes": usage.free,
                "total_bytes": usage.total,
            })
        except OSError:
            storage.append({"dir_index": idx, "free_bytes": 0, "total_bytes": 0})

    # 增量模式: 有上次缓存 + 版本 > 0
    if incremental and _last_sync_version > 0 and _last_synced_codes:
        added_codes = current_codes - _last_synced_codes
        removed_codes = _last_synced_codes - current_codes

        # 如果没有变化，发送空 diff
        added_files = [f for f in files if f["code"] in added_codes]

        return {
            "type": "diff",
            "base_version": _last_sync_version,
            "added": added_files,
            "removed": list(removed_codes),
            "dir_count": len(BASE_DIRS),
            "dir_fingerprints": dir_fingerprints,
            "storage": storage,
            "scan_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    # 全量模式
    shard_size = 5000
    if len(files) <= shard_size:
        return {
            "type": "full",
            "files": files,
            "shard": {"index": 0, "total": 1},
            "dir_count": len(BASE_DIRS),
            "dir_fingerprints": dir_fingerprints,
            "storage": storage,
            "scan_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    # 多分片的情况，返回列表
    total_shards = (len(files) + shard_size - 1) // shard_size
    shards = []
    for i in range(total_shards):
        chunk = files[i * shard_size: (i + 1) * shard_size]
        shard = {
            "files": chunk,
            "shard": {"index": i, "total": total_shards},
        }
        if i == 0:
            shard["type"] = "full"
            shard["dir_count"] = len(BASE_DIRS)
            shard["dir_fingerprints"] = dir_fingerprints
            shard["storage"] = storage
            shard["scan_time"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        shards.append(shard)

    return shards


# ═══════════════════════════════════════════════════════════════
# 设备鉴权
# ═══════════════════════════════════════════════════════════════

def build_auth_payload() -> dict:
    """构建 HMAC 鉴权消息（timestamp + nonce + signature）"""
    ts = int(time.time())
    nonce = str(uuid.uuid4())

    msg = f"AUTH:{ts}:{nonce}"
    signature = hmac_mod.new(
        DEVICE_TOKEN.encode(), msg.encode(), hashlib.sha256
    ).hexdigest()

    return {
        "device_token": DEVICE_TOKEN,
        "timestamp": ts,
        "nonce": nonce,
        "signature": signature,
        "device_name": socket.gethostname()[:60],
        "pre_scan": True,
        "capabilities": {
            "has_ffprobe": _HAS_FFPROBE,
            "platform": platform.system().lower(),
            "os_version": platform.platform(terse=True),
            "arch": platform.machine(),
            "python_version": platform.python_version(),
            "version": VERSION,
        },
    }


def _handle_token_rotate(payload: dict) -> dict:
    """处理令牌轮换消息，更新 config.json 中的 DEVICE_TOKEN"""
    global DEVICE_TOKEN
    new_token = payload.get("new_token", "")
    if not new_token or len(new_token) < 16:
        return {"status": "write_failed", "error": "Invalid new_token format"}

    try:
        _persist_token(new_token)
        DEVICE_TOKEN = new_token
        logger.info("令牌已轮换")
        return {"status": "accepted"}
    except Exception as e:
        logger.error("令牌轮换失败: %s", e)
        return {"status": "write_failed", "error": sanitize_error(str(e))}


def _handle_open_file(payload: dict) -> None:
    """处理 OPEN_FILE 消息：用系统默认应用打开文件"""
    import subprocess
    import sys

    rel_path = payload.get("file_path", "")
    if not rel_path:
        logger.warning("OPEN_FILE: 未提供 file_path")
        return

    # 在所有 BASE_DIRS + TARGET_DIRS 中查找文件
    search_dirs = list(BASE_DIRS) + list(TARGET_DIRS)
    abs_path = None
    for base in search_dirs:
        candidate = os.path.join(base, rel_path)
        if os.path.isfile(candidate):
            abs_path = candidate
            break

    # 如果 file_path 本身就是绝对路径且存在，直接使用
    if abs_path is None and os.path.isabs(rel_path) and os.path.isfile(rel_path):
        abs_path = rel_path

    if abs_path is None:
        logger.warning("OPEN_FILE: 文件不存在 — %s", rel_path)
        return

    logger.info("OPEN_FILE: 打开 %s", abs_path)
    try:
        if sys.platform == "darwin":
            subprocess.Popen(["open", abs_path])
        elif sys.platform == "win32":
            os.startfile(abs_path)  # type: ignore[attr-defined]
        else:
            subprocess.Popen(["xdg-open", abs_path])
    except Exception as e:
        logger.error("OPEN_FILE: 打开失败 — %s", e)


# ═══════════════════════════════════════════════════════════════
# 任务执行引擎
# ═══════════════════════════════════════════════════════════════

def is_path_safe(rel_path: str, base_dir: str | None = None) -> bool:
    """验证相对路径安全性（禁止绝对路径/穿越/符号链接攻击）"""
    if not rel_path:
        return False
    # 禁止空字节
    if "\x00" in rel_path:
        return False
    # 禁止绝对路径
    if rel_path.startswith("/") or re.match(r"^[A-Za-z]:\\", rel_path):
        return False
    # 禁止路径穿越
    normalized = os.path.normpath(rel_path)
    if normalized.startswith(".."):
        return False
    # realpath 验证（解析 symlink 后仍在 base_dir 内）
    if base_dir is not None:
        real_base = os.path.realpath(base_dir)
        real_target = os.path.realpath(os.path.join(base_dir, rel_path))
        if not real_target.startswith(real_base + os.sep) and real_target != real_base:
            return False
    return True


def sanitize_error(msg: str) -> str:
    """脱敏错误消息（移除绝对路径，限制 500 字符）"""
    msg = re.sub(r"[A-Z]:\\(?:[^\\]+\\)+", ".../", msg)
    msg = re.sub(r"/(?:[^/]+/){2,}", ".../", msg)
    return msg[:500]


_ON_CONFLICT_WHITELIST = {"skip", "rename", "overwrite"}


def _validate_task_payload(payload: dict) -> str | None:
    """校验任务参数（task_id/action/on_conflict），返回错误或 None"""
    task_id = payload.get("task_id")
    if not isinstance(task_id, int) or task_id <= 0:
        return "Invalid task_id"
    action = payload.get("action", "")
    if action not in {"MOVE", "SCAN", "ORGANIZE"}:
        return f"Unknown action: {action}"
    # 所有路径字段禁止空字节
    params = payload.get("params", {})
    for key, val in params.items():
        if isinstance(val, str) and "\x00" in val:
            return f"Null byte in param '{key}'"
    # MOVE 的 on_conflict 白名单
    if action == "MOVE":
        oc = params.get("on_conflict", "skip")
        if oc not in _ON_CONFLICT_WHITELIST:
            return f"Invalid on_conflict: {oc}"
    return None


def execute_task(payload: dict, progress_q: queue.Queue | None = None) -> dict:
    """执行任务（MOVE/SCAN），返回 {task_id, status, result/error}"""
    task_id = payload.get("task_id")
    action = payload.get("action", "")
    params = payload.get("params", {})

    def _report_progress(progress: int, msg: str = ""):
        if progress_q is not None:
            progress_q.put({"task_id": task_id, "progress": progress, "msg": msg})

    # 基础校验
    err = _validate_task_payload(payload)
    if err:
        return {"task_id": task_id, "status": "FAILED", "error": err}

    try:
        if action == "MOVE":
            return _execute_move(task_id, params, _report_progress)
        elif action == "SCAN":
            return _execute_scan(task_id, params)
        elif action == "ORGANIZE":
            return _execute_organize(task_id, params)
        else:
            return {
                "task_id": task_id,
                "status": "FAILED",
                "error": f"Unknown action: {action}",
            }
    except Exception as e:
        return {
            "task_id": task_id,
            "status": "FAILED",
            "error": sanitize_error(str(e)),
        }

# 系统/工具产生的垃圾文件，清理目录时视为不存在
_JUNK_FILES = {".DS_Store", ".ds_store", "Thumbs.db", "thumbs.db", "desktop.ini", ".Spotlight-V100", ".fseventsd"}


def _cleanup_empty_dirs(moves: list[dict]) -> int:
    """清理移动后留下的空目录树

    从源文件的父目录向上逐级检查，直到 base_dir（不含）。
    若目录中仅剩系统垃圾文件（.DS_Store 等），先删除垃圾文件再删目录。

    安全守则：
    - 绝不删除 base_dir 自身
    - realpath 校验防止 symlink 逃逸
    - 任何异常静默跳过
    """
    cleaned = 0

    # 收集所有需要检查的 (起始目录, base_dir) 对，去重
    dirs_to_check: list[tuple[Path, Path]] = []
    seen: set[str] = set()

    for m in moves:
        src: Path = m["src"]
        if src.exists():
            continue  # 文件未被移走，跳过

        # 找到此源文件对应的 base_dir
        src_base = None
        for bd in BASE_DIRS:
            try:
                src.relative_to(bd)
                src_base = Path(bd)
                break
            except ValueError:
                continue
        if not src_base:
            continue

        start_dir = src.parent
        key = str(start_dir)
        if key not in seen:
            seen.add(key)
            dirs_to_check.append((start_dir, src_base))

    # 按路径深度降序排列，确保最深的目录先处理
    dirs_to_check.sort(key=lambda x: len(x[0].parts), reverse=True)

    deleted_dirs: set[str] = set()

    for start_dir, src_base in dirs_to_check:
        current = start_dir
        real_base = os.path.realpath(str(src_base))

        while current != src_base and current != current.parent:
            dir_str = str(current)

            # 已删除过则直接向上
            if dir_str in deleted_dirs:
                current = current.parent
                continue

            # 目录已不存在（被前面的循环删除了）
            if not current.exists():
                deleted_dirs.add(dir_str)
                current = current.parent
                continue

            # symlink 逃逸检查
            real_current = os.path.realpath(dir_str)
            if not real_current.startswith(real_base + os.sep) and real_current != real_base:
                break

            try:
                children = list(current.iterdir())
            except OSError:
                break

            # 判断目录是否"有效为空"（只包含垃圾文件或空子目录）
            has_real_content = False
            junk_to_remove: list[Path] = []

            for child in children:
                if child.is_file() or child.is_symlink():
                    if child.name in _JUNK_FILES:
                        junk_to_remove.append(child)
                    else:
                        has_real_content = True
                        break
                elif child.is_dir():
                    if str(child) in deleted_dirs:
                        continue  # 已删除的子目录，忽略
                    has_real_content = True
                    break

            if has_real_content:
                break  # 目录有实际内容，停止向上

            # 先删垃圾文件，再删目录
            try:
                for junk in junk_to_remove:
                    junk.unlink(missing_ok=True)
                current.rmdir()
                deleted_dirs.add(dir_str)
                cleaned += 1
            except OSError:
                break

            current = current.parent

    return cleaned


def _execute_move(task_id: int, params: dict, report_progress=None, target_base: str = None) -> dict:
    """执行移动任务（支持跨盘、冲突处理）"""
    code = params.get("code", "")
    target_dir = params.get("target_dir", "")
    on_conflict = params.get("on_conflict", "skip")

    if not code or not target_dir:
        return {"task_id": task_id, "status": "FAILED", "error": "Missing code or target_dir"}

    if not is_path_safe(target_dir):
        return {"task_id": task_id, "status": "FAILED", "error": "Unsafe target path"}

    # 查找源文件
    source_files = []
    for base_dir in BASE_DIRS:
        root = Path(base_dir)
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in VIDEO_EXTENSIONS:
                continue
            if extract_code_from_filename(path.name) == code:
                source_files.append((path, root))

    if not source_files:
        return {"task_id": task_id, "status": "FAILED", "error": f"File not found: {code}"}

    moves = []
    for src_path, base_root in source_files:
        # 使用指定的 target_base 或源文件的 base_root
        actual_base = Path(target_base) if target_base else base_root
        dest_dir = actual_base / target_dir
        if len(source_files) == 1:
            dest_name = f"{code}{src_path.suffix}"
        else:
            dest_name = src_path.name
        dest_path = dest_dir / dest_name

        rel_from = src_path.relative_to(base_root).as_posix()
        # 如果跨盘，dest 相对于 actual_base；否则相对于 base_root
        try:
            rel_to = dest_path.relative_to(base_root).as_posix()
        except ValueError:
            rel_to = dest_path.relative_to(actual_base).as_posix()
        moves.append({"from": rel_from, "to": rel_to, "src": src_path, "dest": dest_path})

    # 执行移动
    moved = 0
    skipped = 0
    total = len(moves)
    for idx, m in enumerate(moves):
        dest: Path = m["dest"]
        src: Path = m["src"]
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dest.exists():
            if on_conflict == "skip":
                skipped += 1
                continue
            elif on_conflict == "rename":
                counter = 1
                stem = dest.stem
                while dest.exists():
                    dest = dest.with_name(f"{stem}_{counter}{dest.suffix}")
                    counter += 1
            elif on_conflict == "overwrite":
                # 仅当源文件 size >= 目标文件时才覆盖
                if src.stat().st_size < dest.stat().st_size:
                    skipped += 1
                    continue

        # EXDEV 跨文件系统降级，带重试（应对临时文件锁）
        max_retries = 3
        for attempt in range(max_retries):
            try:
                src.rename(dest)
                break
            except OSError as e:
                if e.errno == errno_mod.EXDEV:
                    tmp_dest = dest.with_name(dest.name + ".dmm-tmp")
                    try:
                        shutil.copy2(str(src), str(tmp_dest))
                        if tmp_dest.stat().st_size != src.stat().st_size:
                            tmp_dest.unlink(missing_ok=True)
                            raise OSError(f"Size mismatch after cross-fs copy for {src.name}")
                        tmp_dest.rename(dest)
                        src.unlink()
                    except Exception:
                        tmp_dest.unlink(missing_ok=True)
                        raise
                    break
                elif attempt < max_retries - 1:
                    wait = 2 ** attempt  # 1s, 2s
                    logger.info("文件被锁定，%ds 后重试 (%d/%d): %s", wait, attempt + 1, max_retries, src.name)
                    time.sleep(wait)
                else:
                    raise
        moved += 1
        if report_progress and total > 0:
            pct = min(99, int((idx + 1) / total * 100))
            report_progress(pct, f"Moving file {idx + 1}/{total}...")

    # 清理移动后留下的空目录（bottom-up，忽略系统垃圾文件）
    cleaned_dirs = _cleanup_empty_dirs(moves)

    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "result": {"moved": moved, "skipped": skipped, "cleaned_dirs": cleaned_dirs},
    }


def _execute_scan(task_id: int, params: dict) -> dict:
    """执行扫描任务，返回文件清单 + 去重番号列表"""
    if not BASE_DIRS:
        return {"task_id": task_id, "status": "FAILED", "error": "未配置扫描目录 (BASE_DIRS)"}
    files = scan_local_files(include_target=False, skip_probe=True)
    codes = list(dict.fromkeys(f["code"] for f in files))
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "result": {
            "files_found": len(files),
            "files": files,
            "codes": codes,
        },
    }


def _execute_organize(task_id: int, params: dict) -> dict:
    """执行整理任务：使用 Cloud 下发的元数据整理文件到标准目录结构"""
    metadata = params.get("metadata", {})
    if not metadata:
        return {"task_id": task_id, "status": "SUCCESS", "result": {
            "organized": 0, "skipped": 0, "failed": 0,
            "organized_codes": [], "skipped_codes": [],
        }}

    if not TARGET_DIRS:
        return {"task_id": task_id, "status": "FAILED", "error": "TARGET_DIRS not configured"}

    target_base = TARGET_DIRS[0]
    files = scan_local_files(include_target=False, skip_probe=True)
    # 按 code 索引文件
    code_to_file = {}
    for f in files:
        code_to_file[f["code"]] = f

    stats: dict = {"organized": 0, "skipped": 0, "failed": 0, "organized_codes": [], "skipped_codes": []}

    for code, meta in metadata.items():
        if code not in code_to_file:
            stats["skipped"] += 1
            stats["skipped_codes"].append(code)
            continue

        file_info = code_to_file[code]
        actress = sanitize_dirname(meta.get("actress", "未分类"))
        series = sanitize_dirname(meta.get("series", ""))
        target_dir = ORGANIZE_PATTERN.format(actress=actress, code=code, series=series)

        # 检查是否已在目标位置
        current_path = file_info["paths"][0] if file_info["paths"] else ""
        expected_prefix = str(Path(target_base) / target_dir)
        is_organized = False
        for base_dir in BASE_DIRS:
            abs_current = str(Path(base_dir) / current_path)
            if abs_current.startswith(expected_prefix + os.sep) or abs_current.startswith(expected_prefix):
                is_organized = True
                break

        if is_organized:
            stats["skipped"] += 1
            stats["skipped_codes"].append(code)
            continue

        try:
            result = _execute_move(
                task_id=-1,
                params={"code": code, "target_dir": target_dir, "on_conflict": ORGANIZE_ON_CONFLICT},
                target_base=target_base,
            )
        except Exception as e:
            stats["failed"] += 1
            logger.warning("整理异常 %s: %s", code, e)
            continue

        if result["status"] == "SUCCESS":
            moved = result["result"].get("moved", 0)
            stats["organized"] += moved
            stats["skipped"] += result["result"].get("skipped", 0)
            if moved > 0:
                stats["organized_codes"].append(code)
        else:
            stats["failed"] += 1
            logger.warning("整理失败 %s: %s", code, result.get("error", ""))

    if stats["organized"] > 0 or stats["failed"] > 0:
        logger.info("ORGANIZE 完成: organized=%d skipped=%d failed=%d",
                    stats["organized"], stats["skipped"], stats["failed"])

    return {"task_id": task_id, "status": "SUCCESS", "result": stats}



# ═══════════════════════════════════════════════════════════════
# WebSocket 会话管理
# ═══════════════════════════════════════════════════════════════

async def ws_session():
    """单次 WebSocket 会话（鉴权→扫描→自动整理→消息循环）"""

    try:
        import websockets
    except ImportError:
        logger.error("缺少 websockets 库，请运行: pip install websockets")
        sys.exit(1)

    global _ws_connection

    logger.info("正在连接 %s ...", CLOUD_WS_URL)

    connect_kwargs: dict = {}
    if hasattr(websockets, "__version__") and int(websockets.__version__.split(".")[0]) >= 14:
        connect_kwargs["proxy"] = None  # 禁用代理，避免用户系统代理干扰
    async with websockets.connect(CLOUD_WS_URL, **connect_kwargs) as _raw_ws:

        # 包装 send/recv，统一打印收发消息
        _original_send = _raw_ws.send
        _original_recv = _raw_ws.recv

        async def _logged_send(data):
            try:
                parsed = json.loads(data) if isinstance(data, str) else data
                msg_type = parsed.get("type", "?")
                payload = parsed.get("payload", {})
                # PING/PONG 和 SYNC_REPORT 只打类型，避免刷屏
                if msg_type in ("PING", "PONG"):
                    logger.debug(">>> %s", msg_type)
                elif msg_type == "SYNC_REPORT":
                    shard = payload.get("shard", "")
                    count = len(payload.get("files", []))
                    logger.info(">>> %s (shard=%s, files=%d)", msg_type, shard, count)
                elif msg_type == "TASK_RESULT":
                    status = payload.get("status", "")
                    tid = payload.get("task_id", "")
                    logger.info(">>> %s task=#%s status=%s", msg_type, tid, status)
                elif msg_type == "TASK_PROGRESS":
                    tid = payload.get("task_id", "")
                    pct = payload.get("progress", "")
                    logger.debug(">>> %s task=#%s %s%%", msg_type, tid, pct)
                else:
                    logger.info(">>> %s %s", msg_type, json.dumps(payload, ensure_ascii=False)[:200])
            except Exception:
                logger.info(">>> (raw) %s", str(data)[:200])
            return await _original_send(data)

        async def _logged_recv():
            data = await _original_recv()
            try:
                parsed = json.loads(data) if isinstance(data, str) else data
                msg_type = parsed.get("type", "?")
                payload = parsed.get("payload", {})
                if msg_type in ("PING", "PONG"):
                    logger.debug("<<< %s", msg_type)
                elif msg_type == "EXEC_TASK":
                    action = payload.get("action", "")
                    tid = payload.get("task_id", "")
                    logger.info("<<< %s task=#%s action=%s", msg_type, tid, action)
                elif msg_type in ("SYNC_ACK", "SYNC_REJECT"):
                    ver = payload.get("sync_version", "")
                    logger.info("<<< %s sync_version=%s", msg_type, ver)
                else:
                    logger.info("<<< %s %s", msg_type, json.dumps(payload, ensure_ascii=False)[:200])
            except Exception:
                logger.info("<<< (raw) %s", str(data)[:200])
            return data

        _raw_ws.send = _logged_send
        _raw_ws.recv = _logged_recv
        ws = _raw_ws
        _ws_connection = ws

        # Phase 1: AUTH
        auth_msg = json.dumps({"type": "AUTH", "payload": build_auth_payload()})
        await ws.send(auth_msg)

        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            resp = json.loads(raw)
        except asyncio.TimeoutError:
            logger.error("鉴权超时")
            return

        if resp.get("type") not in ("AUTH_OK", "AUTH_ACK"):
            reason = resp.get("payload", {}).get("reason", "unknown")
            logger.error("鉴权失败: %s", reason)
            if reason in _PERMANENT_AUTH_REASONS:
                raise TokenInvalidError(reason)
            return

        device_id = resp.get("payload", {}).get("device_id")
        logger.info("✓ 已连接 (设备 ID: %s)", device_id)

        # 更新哨兵文件 device_id
        if device_id is not None:
            update_sentinel_device_id(device_id)

        # Phase 2: 初始扫描 + 同步
        # 等待 SYNC_ACK/SYNC_REJECT 期间可能收到其他消息（如 EXEC_TASK），
        # 需要缓存起来在 Phase 3 处理，否则会被丢弃。
        _buffered_messages: list[str] = []

        async def _recv_until_sync_ack():
            """读取消息直到收到 SYNC_ACK/SYNC_REJECT，其他消息缓存"""
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                msg_type = msg.get("type", "")
                if msg_type in ("SYNC_ACK", "SYNC_REJECT", "SYNC_SHARD_ACK"):
                    return msg
                _buffered_messages.append(raw)

        async def _send_sync_report(report, await_ack=False):
            shards = report if isinstance(report, list) else [report]
            if len(shards) > 1:
                logger.info("同步报告: %d 分片", len(shards))
            for shard in shards:
                await ws.send(json.dumps({"type": "SYNC_REPORT", "payload": shard}))
                if await_ack:
                    await _recv_until_sync_ack()

        # 心跳提前启动（Phase 2 扫描可能阻塞数分钟，需保持心跳避免 Server 超时断连）
        async def heartbeat_loop():
            while not _shutdown_event.is_set():
                jitter = random.uniform(-5, 5)
                await asyncio.sleep(HEARTBEAT_INTERVAL + jitter)
                if _shutdown_event.is_set():
                    break
                try:
                    await ws.send(json.dumps({"type": "PING", "payload": {}}))
                except Exception:
                    break

        heartbeat_task = asyncio.create_task(heartbeat_loop())

        if BASE_DIRS:
            loop = asyncio.get_event_loop()

            # 尝试加载扫描缓存（跳过耗时的全量扫描）
            cache = _load_scan_cache()
            if cache and cache.get("complete"):
                cached_files = cache["files"]
                cached_version = cache.get("sync_version", 0)
                logger.info("命中扫描缓存: %d 个番号 (version=%d)", len(cached_files), cached_version)

                # 恢复增量同步状态
                if cached_version > 0:
                    _last_sync_version = cached_version
                    _last_synced_codes = {f["code"] for f in cached_files}

                # 发送完成进度
                try:
                    await ws.send(json.dumps({
                        "type": "PRE_SCAN_STATUS",
                        "payload": {"scanned": len(cached_files), "total": len(cached_files)},
                    }))
                except Exception:
                    pass

                logger.info("扫描完成: %d 个番号 (cached)", len(cached_files))
                report = await loop.run_in_executor(
                    None, lambda: build_sync_report(prefetched_files=cached_files),
                )
                await _send_sync_report(report, await_ack=True)
            else:
                # 无缓存或缓存不完整 → 全量扫描（带增量刷盘）
                if cache and not cache.get("complete"):
                    logger.info("发现不完整缓存 (%d 个番号)，继续全量扫描", len(cache.get("files", [])))

                # 预扫描进度上报：线程安全计数器 + async 定时发送
                _pre_scan_count = 0
                _pre_scan_done = False

                def _on_scan_progress(count: int):
                    nonlocal _pre_scan_count
                    _pre_scan_count = count

                async def _pre_scan_reporter():
                    """每 2 秒发送一次 PRE_SCAN_STATUS"""
                    last_sent = 0
                    while not _pre_scan_done:
                        await asyncio.sleep(2)
                        current = _pre_scan_count
                        if current != last_sent:
                            try:
                                await ws.send(json.dumps({
                                    "type": "PRE_SCAN_STATUS",
                                    "payload": {"scanned": current, "total": 0},
                                }))
                            except Exception:
                                break
                            last_sent = current

                reporter_task = asyncio.create_task(_pre_scan_reporter())
                try:
                    my_files = await loop.run_in_executor(
                        None, lambda: scan_local_files(
                            include_target=False,
                            on_progress=_on_scan_progress,
                            flush_cache=True,
                        ),
                    )
                finally:
                    _pre_scan_done = True
                    reporter_task.cancel()
                    try:
                        await reporter_task
                    except asyncio.CancelledError:
                        pass

                # 发送最终进度
                try:
                    await ws.send(json.dumps({
                        "type": "PRE_SCAN_STATUS",
                        "payload": {"scanned": len(my_files), "total": len(my_files)},
                    }))
                except Exception:
                    pass

                logger.info("扫描完成: %d 个番号", len(my_files))
                report = await loop.run_in_executor(None, build_sync_report)
                await _send_sync_report(report, await_ack=True)

        # Phase 3: 心跳 + 任务队列 + 消息循环
        task_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        TASK_TIMEOUT = 300  # 5 分钟任务超时

        async def task_worker():
            """单 worker 顺序执行任务（保证文件操作串行化）"""
            global _last_synced_codes, _my_organized_codes

            async def _flush_progress(pq):
                while not pq.empty():
                    try:
                        prog = pq.get_nowait()
                        await ws.send(json.dumps({"type": "TASK_PROGRESS", "payload": prog}))
                    except (queue.Empty, Exception):
                        break

            while not _shutdown_event.is_set():
                try:
                    payload = await asyncio.wait_for(task_queue.get(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                task_id = payload.get("task_id")
                action = payload.get("action")
                logger.info("执行任务: #%s %s", task_id, action)
                progress_q = queue.Queue()
                loop = asyncio.get_event_loop()
                future = loop.run_in_executor(None, execute_task, payload, progress_q)
                deadline = loop.time() + TASK_TIMEOUT
                result = None
                try:
                    while True:
                        await _flush_progress(progress_q)
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            future.cancel()
                            result = {"task_id": task_id, "status": "FAILED", "error": "Task timeout (5min)"}
                            break
                        try:
                            result = await asyncio.wait_for(asyncio.shield(future), timeout=min(0.5, remaining))
                            break
                        except asyncio.TimeoutError:
                            continue
                except Exception as e:
                    result = {"task_id": task_id, "status": "FAILED", "error": sanitize_error(str(e))}
                await _flush_progress(progress_q)

                status = result.get("status") if result else "None"
                if status == "SUCCESS":
                    logger.info("任务完成: #%s", task_id)
                elif status == "FAILED":
                    logger.warning("任务失败: #%s - %s", task_id, result.get("error", ""))

                # ORGANIZE 成功 → 记录已整理的 code + 发送全量 SYNC 更新路径
                if action == "ORGANIZE" and status == "SUCCESS":
                    organized_codes = result.get("result", {}).get("organized_codes", [])
                    if organized_codes:
                        _my_organized_codes.update(organized_codes)
                        await _send_sync_report(build_sync_report(incremental=False))

                try:
                    await ws.send(json.dumps({"type": "TASK_RESULT", "payload": result}))
                except Exception:
                    break
                task_queue.task_done()

        async def message_loop():
            global _last_sync_version, _last_synced_codes
            # 先处理 Phase 2 期间缓存的消息（如 EXEC_TASK）
            for buffered_raw in _buffered_messages:
                try:
                    msg = json.loads(buffered_raw)
                except json.JSONDecodeError:
                    continue
                msg_type = msg.get("type", "")
                payload = msg.get("payload", {})
                if msg_type == "EXEC_TASK":
                    try:
                        task_queue.put_nowait(payload)
                    except asyncio.QueueFull:
                        pass
            _buffered_messages.clear()

            while True:
                try:
                    raw = await ws.recv()
                except websockets.exceptions.ConnectionClosed:
                    break
                except Exception as e:
                    logger.warning("接收消息异常: %s", e)
                    break
                if _shutdown_event.is_set():
                    break

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                msg_type = msg.get("type", "")
                payload = msg.get("payload", {})

                if msg_type == "PONG":
                    pass  # 心跳响应

                elif msg_type == "EXEC_TASK":
                    try:
                        task_queue.put_nowait(payload)
                    except asyncio.QueueFull:
                        task_id = payload.get("task_id")
                        logger.warning("任务队列已满，拒绝任务 #%s", task_id)
                        await ws.send(json.dumps({
                            "type": "TASK_RESULT",
                            "payload": {"task_id": task_id, "status": "FAILED", "error": "Task queue full"},
                        }))

                elif msg_type == "TASK_CANCEL":
                    task_id = payload.get("task_id")
                    await ws.send(json.dumps({
                        "type": "TASK_RESULT",
                        "payload": {"task_id": task_id, "status": "FAILED", "error": "cancelled"},
                    }))

                elif msg_type == "SYNC_ACK":
                    # 更新增量同步状态
                    new_version = payload.get("sync_version", 0)
                    if new_version > 0:
                        _last_sync_version = new_version
                        # 重新扫描获取当前 codes 作为下次 diff 基准
                        current_files = _get_my_files()
                        _last_synced_codes = {f["code"] for f in current_files}
                        # 持久化完整缓存（下次启动跳过扫描 + 增量 diff）
                        _save_scan_cache(current_files, sync_version=new_version, complete=True)

                elif msg_type == "SYNC_SHARD_ACK":
                    pass  # 分片确认

                elif msg_type == "SYNC_REJECT":
                    reason = payload.get("reason", "unknown")
                    logger.warning("同步被拒绝: %s，重传全量", reason)
                    _last_sync_version = 0
                    _last_synced_codes = set()
                    _invalidate_scan_cache()
                    await _send_sync_report(build_sync_report(incremental=False))

                elif msg_type == "TOKEN_ROTATE":
                    ack = _handle_token_rotate(payload)
                    await ws.send(json.dumps({"type": "TOKEN_ROTATE_ACK", "payload": ack}))

                elif msg_type == "OPEN_FILE":
                    _handle_open_file(payload)

                elif msg_type == "DEVICE_DELETED":
                    logger.warning("设备已被删除，退出运行")
                    _shutdown_event.set()
                    break

        worker_task = asyncio.create_task(task_worker())
        message_task = asyncio.create_task(message_loop())

        # 等待信号或连接断开
        done, pending = await asyncio.wait(
            [heartbeat_task, worker_task, message_task, asyncio.create_task(_shutdown_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        # 发送 GOODBYE
        if not ws.closed:
            try:
                await ws.send(json.dumps({
                    "type": "GOODBYE",
                    "payload": {"reason": "shutdown"},
                }))
            except Exception:
                pass

        _ws_connection = None


async def run_forever():
    """主循环（连接→断线重连→指数退避）"""

    global DEVICE_TOKEN, _shutdown_event
    attempt = 0

    # 在 asyncio 事件循环内注册信号，确保能唤醒 select()
    loop = asyncio.get_running_loop()
    _shutdown_event = asyncio.Event()

    def _async_signal_handler():
        logger.info("收到退出信号，正在关闭...")
        _shutdown_event.set()

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _async_signal_handler)
    else:
        # Windows: signal.signal fallback（在主线程中 Ctrl+C 触发 KeyboardInterrupt）
        signal.signal(signal.SIGINT, lambda *_: _async_signal_handler())

    while True:
        try:
            await ws_session()
            attempt = 0  # 成功连接后重置
        except TokenInvalidError as e:
            logger.warning("令牌失效 (%s)，重新绑定", e)
            DEVICE_TOKEN = dev_auto_bind() if IS_DEV else first_time_setup()
            _persist_token(DEVICE_TOKEN)
            attempt = 0
            continue
        except Exception as e:
            logger.warning("连接断开: %s", e)

        if _shutdown_event and _shutdown_event.is_set():
            break

        # 指数退避 + 抖动
        delay = min(RECONNECT_BASE * (2 ** attempt), RECONNECT_MAX)
        jitter = random.uniform(-RECONNECT_JITTER, RECONNECT_JITTER)
        wait = max(1, delay + jitter)
        attempt += 1

        logger.info("%.1fs 后重连 (#%d)", wait, attempt)
        await asyncio.sleep(wait)


# ═══════════════════════════════════════════════════════════════
# 信号处理与优雅退出
# ═══════════════════════════════════════════════════════════════

def setup_signal_handlers():
    """兜底信号处理：第二次 Ctrl+C 强制退出（asyncio 信号在 run_forever 中注册）"""
    _ctrl_c_count = 0

    def handler(signum, frame):
        nonlocal _ctrl_c_count
        _ctrl_c_count += 1
        if _ctrl_c_count >= 2:
            logger.info("强制退出")
            os._exit(1)
        logger.info("收到退出信号，正在关闭...（再按一次强制退出）")
        if _shutdown_event:
            _shutdown_event.set()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


# ═══════════════════════════════════════════════════════════════
# 设备绑定流程
# ═══════════════════════════════════════════════════════════════

def _derive_api_base() -> str:
    """从 WebSocket URL 推导 HTTP API 基础地址"""
    base = CLOUD_WS_URL
    base = base.replace("wss://", "https://").replace("ws://", "http://")
    # 去掉 /ws/agent 路径
    if base.endswith("/ws/agent"):
        base = base[: -len("/ws/agent")]
    return base


def _persist_token(new_token: str) -> None:
    """持久化令牌到配置文件"""
    try:
        cfg_path = Path(_CONFIG_FILE)
        if cfg_path.exists():
            existing = _json_mod.loads(cfg_path.read_text(encoding="utf-8"))
        else:
            existing = {}
        existing["DEVICE_TOKEN"] = new_token
        cfg_path.write_text(
            _json_mod.dumps(existing, indent=4, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        logger.info("✓ 令牌已保存至 config.json")
    except Exception as e:
        logger.warning("保存令牌失败: %s", e)


def dev_auto_bind() -> str:
    """开发环境自动绑定（调用 dev-bind 接口，无需手动操作）"""
    import urllib.request
    import urllib.error

    api_base = _derive_api_base()
    bind_url = f"{api_base}/api/v1/auth/dev-bind"

    logger.info("开发环境自动绑定 (tg_id=%s, %s)...", DEV_TG_ID, DEV_USER_NAME)

    bind_body = json.dumps({"tg_id": DEV_TG_ID, "display_name": DEV_USER_NAME}).encode()
    req = urllib.request.Request(
        bind_url,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=bind_body,
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            device_token = data["device_token"]
            logger.info("✓ 绑定成功 (设备 ID: %s)", data.get("device_id"))
            return device_token
    except urllib.error.HTTPError as e:
        if e.code == 403:
            logger.error("自动绑定失败: 后端未启用 DEBUG 模式")
        else:
            logger.error("自动绑定失败: HTTP %d", e.code)
        sys.exit(1)
    except (urllib.error.URLError, OSError) as e:
        logger.error("无法连接云端: %s", e)
        logger.error("请确保后端服务已启动: ./scripts/dev/dev.sh start")
        sys.exit(1)


def first_time_setup() -> str:
    """生产环境首次绑定（请求绑定码→打印 Telegram 链接→轮询等待）"""
    import urllib.request
    import urllib.error

    api_base = _derive_api_base()

    # Step 1: 请求绑定码
    bind_url = f"{api_base}/api/v1/auth/bind-request"
    req = urllib.request.Request(
        bind_url,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=b"{}",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except (urllib.error.URLError, OSError) as e:
        logger.error("无法连接云端 (%s): %s", bind_url, e)
        sys.exit(1)

    auth_code = data["auth_code"]
    magic_link = data["magic_link"]
    expires_in = data.get("expires_in", 300)

    print()
    print("=" * 56)
    print("  DMM-CM Agent — Device Binding")
    print("=" * 56)
    print()
    print("  Please click the link below to bind this device:")
    print(f"  {magic_link}")
    print()
    print(f"  This link expires in {expires_in // 60} minutes.")
    print("=" * 56)
    print()

    # Step 2: 轮询绑定状态
    status_url = f"{api_base}/api/v1/auth/bind-status?auth_code={auth_code}"
    poll_interval = 3
    elapsed = 0

    while elapsed < expires_in:
        time.sleep(poll_interval)
        elapsed += poll_interval

        try:
            with urllib.request.urlopen(status_url, timeout=10) as resp:
                result = json.loads(resp.read())
        except (urllib.error.URLError, OSError):
            continue  # 网络临时问题，继续轮询

        bind_status = result.get("status")

        if bind_status == "completed":
            device_token = result["device_token"]
            logger.info("Device binding successful!")
            return device_token
        elif bind_status == "expired":
            logger.error("Bind code expired. Please restart the agent.")
            sys.exit(1)
        # "pending" → 继续轮询

    logger.error("Bind timed out after %d seconds. Please restart the agent.", expires_in)
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════

def main():
    """Agent 启动入口（配置校验→自动绑定→主循环）"""

    global DEVICE_TOKEN

    # 配置校验
    if not CLOUD_WS_URL.startswith(("ws://", "wss://")):
        logger.error("CLOUD_WS_URL 配置错误，必须以 ws:// 或 wss:// 开头")
        sys.exit(1)

    # 前置检查：无 token 时启动自动绑定流程
    if not DEVICE_TOKEN:
        if IS_DEV:
            DEVICE_TOKEN = dev_auto_bind()
        else:
            DEVICE_TOKEN = first_time_setup()
        _persist_token(DEVICE_TOKEN)

    if len(DEVICE_TOKEN) < 16:
        logger.warning("DEVICE_TOKEN 长度异常，可能无效")

    # 目录检查
    if not BASE_DIRS:
        logger.warning("BASE_DIRS 为空，不会扫描本地文件")
    else:
        for bd in BASE_DIRS:
            p = Path(bd)
            if not p.exists():
                logger.warning("扫描目录不存在: %s", bd)
            elif not p.is_dir():
                logger.warning("扫描路径非目录: %s", bd)

    # root 权限警告
    if hasattr(os, "getuid") and os.getuid() == 0:
        logger.warning("⚠️  以 root 运行有安全风险，建议使用普通用户")

    # 单实例锁
    lock = acquire_singleton_lock()
    logger.info("=" * 60)
    logger.info("DMM-CM Agent v%s", VERSION)
    if os.path.exists(_CONFIG_FILE):
        logger.info("配置: %s%s", _CONFIG_FILE, " (DEV)" if IS_DEV else "")
    else:
        logger.warning("配置: 内置默认（未找到 config.json，请参考 config.example.json 创建）")
    logger.info("云端: %s", CLOUD_WS_URL)
    logger.info("扫描: %s", ", ".join(BASE_DIRS) if BASE_DIRS else "(未配置)")
    if _HAS_FFPROBE:
        logger.info("ffprobe: ✓ (%s)", _FFPROBE_CMD)
    else:
        logger.warning("ffprobe: ✗ (未检测到，视频元数据将不可用。FFPROBE_PATH=%r)", FFPROBE_PATH)
    logger.info("=" * 60)

    setup_signal_handlers()

    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        pass
    finally:
        os.close(lock)
        logger.info("Agent 已退出")


if __name__ == "__main__":
    main()
