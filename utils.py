"""纯工具函数：时间格式化与服务器状态采集。

从 main.py 拆出的无状态函数，不依赖插件实例状态，便于单独测试。
"""

from __future__ import annotations

import os
import platform
import socket
import time

import psutil

from astrbot import __version__ as ASTRBOT_VERSION


def fmt_uptime(seconds: float) -> str:
    s = max(int(seconds), 0)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m = s // 60
    if d > 0:
        return f"{d} 天 {h} 小时"
    if h > 0:
        return f"{h} 小时 {m} 分"
    return f"{m} 分"


def fmt_age(seconds: float) -> str:
    s = max(int(seconds), 0)
    if s < 60:
        return f"{s} 秒"
    if s < 3600:
        return f"{s // 60} 分"
    return f"{s // 3600} 小时"


def fmt_duration(seconds: int) -> str:
    if seconds <= 0:
        return "已到期"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if h > 0:
        return f"{h} 小时 {m} 分"
    return f"{m} 分"


def server_text() -> list[tuple[str, str]]:
    """Reads host stats. May block (~0.5s CPU sampling) — call via asyncio.to_thread."""
    try:
        cpu = psutil.cpu_percent(interval=0.5)
        cores = psutil.cpu_count() or "?"
        mem = psutil.virtual_memory()
        mem_txt = f"{mem.used / 2**30:.1f} GB / {mem.total / 2**30:.1f} GB ({mem.percent:.0f}%)"
        disk_txt = "-"
        # Platform-aware system-disk detection: drive letters on Windows,
        # the root mountpoint on POSIX (both may return more mounts than that).
        for part in psutil.disk_partitions():
            mp = part.mountpoint
            if not part.fstype or not mp:
                continue
            if os.name == "nt":
                if not mp.endswith("\\"):
                    continue
            elif mp != "/":
                continue
            try:
                usage = psutil.disk_usage(mp)
            except OSError:
                continue
            disk_txt = (
                f"{usage.used / 2**30:.0f} GB / {usage.total / 2**30:.0f} GB "
                f"({usage.percent:.0f}%)"
            )
            break
        uptime = fmt_uptime(time.time() - psutil.boot_time())
        proc = psutil.Process()
        astrbot_uptime = fmt_uptime(time.time() - proc.create_time())
        host = socket.gethostname()
        sys_txt = f"{platform.system()} {platform.release()}"
        return [
            ("主机", f"{host} · {sys_txt}"),
            ("CPU", f"{cpu:.0f}% · {cores} 核"),
            ("内存", mem_txt),
            ("磁盘", disk_txt),
            ("开机", uptime),
            ("AstrBot", f"v{ASTRBOT_VERSION} · 运行 {astrbot_uptime}"),
        ]
    except Exception as e:
        return [("错误", f"读取失败: {e}")]


def format_server_text(fields: list[tuple[str, str]]) -> str:
    lines = ["## 🖥️ 服务器状态"]
    for key, value in fields:
        lines.append(f"**{key}**：{value}")
    lines.append(f"> 查询时间 {time.strftime('%m-%d %H:%M:%S')}")
    return "\n".join(lines)
