from __future__ import annotations

import asyncio
import json
import re
import secrets
import time
import zipfile
from pathlib import Path

from aiohttp import web

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.event.filter import CustomFilter
from astrbot.api.message_components import File
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.star.filter.command import GreedyStr

from .utils import fmt_age, fmt_duration, format_server_text, server_text

_PLUGIN_DIR = Path(__file__).resolve().parent
_DATA_DIR = StarTools.get_data_dir("astrbot_plugin_zkq")
_SNAPSHOT_FILE = _DATA_DIR / "snapshots.json"
_SCREENSHOT_DIR = _DATA_DIR / "screenshots"
_KEEP_SCREENSHOTS = 20
_STALE_FACTOR = 3  # snapshot considered stale after 3x recommended interval
_CLEANUP_DAYS = 7  # devices with no update for this long are dropped
_CLEANUP_INTERVAL = 3600  # seconds between stale-device cleanup passes
_QUERY_TIMEOUT = 10  # seconds to wait for a WS query result frame
_QR_POLL_INTERVAL = 15  # seconds between 扫码提取 status polls (ADR-0003)
_QR_SESSION_DEADLINE = 2 * 3600  # 扫码提取会话总时长上限（秒，ADR-0004：多轮会话放宽到 2 小时）
_MAX_SNAPSHOT_BYTES = 1 << 20  # 1 MiB snapshot JSON body cap (memory DoS backstop)
_MAX_WS_CONNS = 128  # live WS connection cap (each legit device holds exactly 1)
_TOKEN_HEADER = "X-Zkq-Token"
_READ_TIMEOUT = 10  # seconds to wait for the snapshot JSON body
_UPLOAD_TIMEOUT = 120  # seconds to wait for a screenshot/logfile upload


def _safe_name(value: str) -> str:
    """Sanitizes a device-supplied string for use inside a filename."""
    cleaned = re.sub(r"[^0-9A-Za-z_一-鿿\-]", "_", value)
    cleaned = cleaned.strip("_.")
    return cleaned or "unknown"


async def _save_uploaded(part, limit_bytes: int, filename: str) -> tuple[str, int] | None:
    """Streams a multipart file part straight to disk under _SCREENSHOT_DIR.

    Bounded only by limit_bytes as a DoS backstop; large uploads (e.g. a year
    of logs) stream through chunk by chunk without buffering in memory.
    Returns (saved_path, byte_count), or None when over the limit (the partial
    file is removed).
    """
    _SCREENSHOT_DIR.mkdir(exist_ok=True)
    path = _SCREENSHOT_DIR / filename
    tmp = _SCREENSHOT_DIR / f"{filename}.tmp"
    total = 0
    try:
        with tmp.open("wb") as fh:
            while True:
                chunk = await part.read_chunk()
                if not chunk:
                    break
                total += len(chunk)
                if total > limit_bytes:
                    return None
                fh.write(chunk)
        tmp.replace(path)
        return str(path), total
    finally:
        tmp.unlink(missing_ok=True)


class ZkqStatus(Star):
    """紫孔雀挂机脚本助手：查询类命令只读；扫码提取会话会操控脚本清数据、链接账号、提取存档。

    支持的用法：
    - /zkq 设备列表、/zkq 状态 <设备>、/zkq 全部：查看设备运行状态
    - /zkq 服务器：查看服务器状态（CPU/内存/磁盘）
    - /zkq 启动 <设备>、/zkq 停止 <设备>：远程启动/停止挂机
    - /zkq 日志 <设备>：查看最近日志（文本）
    - /zkq 日志文件 <设备>：完整日志打包成文件发送
    - /zkq 截图 <设备>：设备实时截图
    - /zkq 扫码提取 <设备> <槽位>：开启提取会话，远程扫码链接账号并提取存档（仅私聊，破坏性）
    - /zkq 登录方式 <设备> <QQ|微信>：提取流程停在登录方式选择界面时选择（仅私聊）
    - /zkq 确认提取 <设备>：村庄截图后确认并提取（仅私聊）
    - /zkq 取消提取 <设备>：中止当前这一轮提取（仅私聊）
    - /zkq 继续提取 <设备> <槽位>：提取会话里继续提下一个账号（仅私聊）
    - /zkq 结束提取 <设备>：结束提取会话，设备自动重启恢复挂机（仅私聊）
    - /zkq ping <设备>：测试设备连接
    - /zkq 清空日志 <设备>：删除日志（仅私聊可用）
    - AI 对话：问"设备在干什么""服务器状态怎么样""截个图看看""把今天的日志发我"，AI 会自动调用工具
    """

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.snapshots: dict[str, dict] = {}  # deviceId -> {snapshot, ts, nonce}
        self.conns: dict[str, web.WebSocketResponse] = {}  # deviceId -> ws
        self._conn_nonce: dict[str, str] = {}  # deviceId -> nonce (for rename migration)
        self._pending: dict[str, dict[int, asyncio.Future]] = {}  # deviceId -> {requestId: future}
        self._request_counter = 0
        # 扫码提取（ADR-0003）：deviceId -> 后台状态轮询任务 / 发起命令的会话事件
        self._qr_tasks: dict[str, asyncio.Task] = {}
        self._qr_events: dict[str, AstrMessageEvent] = {}
        self._bg_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._runner: web.AppRunner | None = None
        self._persist_lock = asyncio.Lock()
        self._pause_start_times: dict[str, float] = {}
        self._pause_warned: dict[str, bool] = {}
        self._last_admin_session: str | None = None
        self._load_snapshots()
        self._tool_desc_sig: tuple | None = None
        self._refresh_tool_descriptions()
        self._ensure_token()
        self._clean_handler_descriptions()
        if config.get("enabled", False):
            self._bg_task = asyncio.create_task(self._run_server())
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info(
            f"[zkq] loaded enabled={config.get('enabled', False)} "
            f"port={config.get('server_port', 16841)} devices={len(self.snapshots)}"
        )

    def _clean_handler_descriptions(self) -> None:
        """优化 WebUI 插件行为卡片中显示的描述文本为精简中文。"""
        try:
            from astrbot.core.star.star_handler import star_handlers_registry
            for full_name, handler_md in star_handlers_registry.star_handlers_map.items():
                if "llm_zkq_assistant" in full_name:
                    handler_md.desc = "紫孔雀脚本助手自然语言管理"
        except Exception:
            pass

    # ── Server (single port: snapshot POST + WS) ─────────────────────
    async def _run_server(self) -> None:
        app = web.Application()
        app.router.add_post("/zkq_snapshot", self._handle_snapshot)
        app.router.add_post("/zkq_screenshot", self._handle_screenshot)
        app.router.add_post("/zkq_logfile", self._handle_logfile)
        app.router.add_get("/zkq_ws", self._handle_ws)
        runner = web.AppRunner(app)
        await runner.setup()
        try:
            site = web.TCPSite(
                runner, "0.0.0.0", int(self.config.get("server_port", 16841))
            )
            await site.start()
        except Exception as e:
            await runner.cleanup()
            logger.error(
                f"[zkq] 通讯服务启动失败：端口 {self.config.get('server_port', 16841)} "
                f"可能被占用，修改后请重载插件。错误：{e}"
            )
            return
        self._runner = runner
        logger.info(
            f"[zkq] server listening on 0.0.0.0:"
            f"{self.config.get('server_port', 16841)} (通讯)"
        )

    # ── Snapshot receiver ────────────────────────────────────────────
    async def _handle_snapshot(self, request: web.Request) -> web.Response:
        try:
            raw = await asyncio.wait_for(
                request.content.read(_MAX_SNAPSHOT_BYTES + 1), timeout=_READ_TIMEOUT
            )
        except asyncio.TimeoutError:
            return web.json_response({"status": "error", "message": "read timeout"}, status=408)
        except Exception:
            return web.json_response({"status": "error", "message": "read failed"})
        if len(raw) > _MAX_SNAPSHOT_BYTES:
            return web.json_response(
                {"status": "error", "message": "payload too large"}, status=413
            )
        try:
            data = json.loads(raw)
        except Exception:
            return web.json_response({"status": "error", "message": "invalid json"})
        if not self._token_ok(request, body_token=data.get("token")):
            return web.json_response({"status": "error", "message": "unauthorized"})

        device_id = str(data.get("deviceId") or "").strip()
        if not device_id:
            return web.json_response({"status": "error", "message": "missing deviceId"})

        nonce = str(data.get("nonce") or "")
        existing = self.snapshots.get(device_id)
        if existing and existing.get("nonce") and nonce and existing["nonce"] != nonce:
            return web.json_response(
                {"status": "error", "code": "device_conflict", "message": "设备名冲突"}
            )

        # Device rename migration: the same nonce under a different deviceId
        # means the device was renamed -> drop the stale snapshot and its WS
        # connection so the device list reflects the new name immediately.
        stale = [
            old_id
            for old_id, rec in self.snapshots.items()
            if old_id != device_id and nonce and rec.get("nonce") == nonce
        ]
        for old_id in stale:
            self.snapshots.pop(old_id, None)
            old_conn = self.conns.pop(old_id, None)
            self._conn_nonce.pop(old_id, None)
            if old_conn and not old_conn.closed:
                await old_conn.close(code=1001, message=b"renamed")
            logger.info(f"[zkq] renamed: {old_id} -> {device_id}")

        try:
            ts = float(data.get("ts") or time.time())
        except (TypeError, ValueError):
            ts = time.time()
        snap = data.get("snapshot") or {}
        if not isinstance(snap, dict):
            snap = {}
        self.snapshots[device_id] = {
            "snapshot": snap,
            "ts": ts,
            "nonce": nonce,
        }
        await self._persist_async()
        sig = tuple(sorted(self.snapshots))
        if sig != self._tool_desc_sig:
            self._refresh_tool_descriptions()
        return web.json_response(
            {
                "status": "ok",
                "recommended_interval": int(self.config.get("snapshot_interval", 60)),
                "log_retention_days": int(self.config.get("log_retention_days", 7)),
            }
        )

    # ── Screenshot receiver (uploaded by the App after a "screenshot" query) ──
    async def _handle_screenshot(self, request: web.Request) -> web.Response:
        logger.info(
            f"[zkq] screenshot upload from device={request.query.get('deviceId')!r} "
            f"nonce={request.query.get('nonce')!r} rid={request.query.get('requestId')!r} "
            f"content-type={request.headers.get('Content-Type')!r}"
        )
        if not self._token_ok(request):
            raise web.HTTPForbidden(text="unauthorized")
        device_id = request.query.get("deviceId", "").strip()
        if not device_id:
            raise web.HTTPBadRequest(text="missing deviceId")
        nonce = request.query.get("nonce", "")
        existing = self.snapshots.get(device_id)
        if existing and existing.get("nonce") and nonce and existing["nonce"] != nonce:
            logger.warning(
                f"[zkq] screenshot rejected: nonce mismatch device={device_id} "
                f"have={existing.get('nonce')!r} got={nonce!r}"
            )
            raise web.HTTPConflict(text="device_conflict")
        request_id = request.query.get("requestId", "0")

        try:
            reader = await request.multipart()
        except Exception as e:
            logger.warning(f"[zkq] screenshot multipart error: {e!r}")
            return web.json_response({"status": "error", "message": f"multipart: {e}"})
        part = await reader.next()
        if part is None or part.name != "file":
            logger.warning(f"[zkq] screenshot missing file part (got {part.name if part else None})")
            return web.json_response({"status": "error", "message": "missing file"})
        try:
            upload_limit = max(1, int(self.config.get("upload_limit_mb", 256) or 256))
        except (TypeError, ValueError):
            upload_limit = 256
        try:
            ext = Path(part.filename or "screenshot.png").suffix or ".png"
            filename = f"screenshot_{_safe_name(request_id)}_{_safe_name(device_id)}_{int(time.time())}{ext}"
            saved = await asyncio.wait_for(
                _save_uploaded(part, upload_limit * 1024 * 1024, filename),
                timeout=_UPLOAD_TIMEOUT,
            )
            if saved is None:
                return web.json_response({"status": "error", "message": "file too large"})
            path, size = saved
            if size == 0:
                path.unlink(missing_ok=True)
                return web.json_response({"status": "error", "message": "empty file"})
            self._cleanup_screenshots()
            logger.info(f"[zkq] screenshot saved: {filename} ({size} bytes)")
            return web.json_response({"status": "ok", "filename": filename})
        except Exception as e:
            logger.error(f"[zkq] save screenshot failed: {e}")
            return web.json_response({"status": "error", "message": str(e)})

    def _cleanup_screenshots(self) -> None:
        """Keeps only the most recent _KEEP_SCREENSHOTS files."""
        try:
            files = sorted(
                list(_SCREENSHOT_DIR.glob("screenshot_*.png"))
                + list(_SCREENSHOT_DIR.glob("screenshot_*.zip"))
                + list(_SCREENSHOT_DIR.glob("logs_*.zip")),
                key=lambda f: f.stat().st_mtime,
            )
            for old in files[: max(0, len(files) - _KEEP_SCREENSHOTS)]:
                old.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"[zkq] cleanup screenshots failed: {e}")

    # ── Log archive receiver (zip of the device's retained log files) ──
    async def _handle_logfile(self, request: web.Request) -> web.Response:
        logger.info(
            f"[zkq] logfile upload from device={request.query.get('deviceId')!r} "
            f"rid={request.query.get('requestId')!r}"
        )
        if not self._token_ok(request):
            raise web.HTTPForbidden(text="unauthorized")
        device_id = request.query.get("deviceId", "").strip()
        if not device_id:
            raise web.HTTPBadRequest(text="missing deviceId")
        nonce = request.query.get("nonce", "")
        existing = self.snapshots.get(device_id)
        if existing and existing.get("nonce") and nonce and existing["nonce"] != nonce:
            raise web.HTTPConflict(text="device_conflict")
        request_id = request.query.get("requestId", "0")
        try:
            reader = await request.multipart()
        except Exception as e:
            logger.warning(f"[zkq] logfile multipart error: {e!r}")
            return web.json_response({"status": "error", "message": f"multipart: {e}"})
        part = await reader.next()
        if part is None or part.name != "file":
            return web.json_response({"status": "error", "message": "missing file"})
        try:
            upload_limit = max(1, int(self.config.get("upload_limit_mb", 256) or 256))
        except (TypeError, ValueError):
            upload_limit = 256
        try:
            filename = f"logs_{_safe_name(request_id)}_{_safe_name(device_id)}_{int(time.time())}.zip"
            saved = await asyncio.wait_for(
                _save_uploaded(part, upload_limit * 1024 * 1024, filename),
                timeout=_UPLOAD_TIMEOUT,
            )
            if saved is None:
                return web.json_response({"status": "error", "message": "file too large"})
            path, size = saved
            if size == 0:
                path.unlink(missing_ok=True)
                return web.json_response({"status": "error", "message": "empty file"})
            self._cleanup_screenshots()
            logger.info(f"[zkq] logfile saved: {filename} ({size} bytes)")
            return web.json_response({"status": "ok", "filename": filename})
        except Exception as e:
            logger.error(f"[zkq] save logfile failed: {e}")
            return web.json_response({"status": "error", "message": str(e)})

    # ── WS endpoint (channel B', doc §13.3/§13.4) ────────────────────
    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        # Handshake auth rejected at the HTTP layer so clients (App okhttp WS)
        # see a clean 403/409/503 instead of an accepted-then-closed socket.
        if not self._token_ok(request):
            raise web.HTTPForbidden(text="unauthorized")
        device_id = request.query.get("deviceId", "").strip()
        if not device_id:
            raise web.HTTPBadRequest(text="missing deviceId")
        nonce = request.query.get("nonce", "")
        existing = self.snapshots.get(device_id)
        if existing and existing.get("nonce") and nonce and existing["nonce"] != nonce:
            # Name conflict with a genuinely different device (doc §13.1):
            # the App auto-renames itself and retries.
            raise web.HTTPConflict(text="device_conflict")
        # Cap on live connections. Reconnects/replacements for an already
        # connected device are allowed; brand-new devices beyond the cap are
        # rejected so an unauthenticated flood cannot exhaust the process.
        if device_id not in self.conns and len(self.conns) >= _MAX_WS_CONNS:
            raise web.HTTPServiceUnavailable(text="too_many_connections")

        # compress=False: aiohttp's permessage-deflate negotiation occasionally
        # conflicts with okhttp's client compression ("Received frame with
        # non-zero reserved bits" protocol error, dropping the socket mid-query).
        # With no extension negotiated the client never sets RSV bits.
        ws = web.WebSocketResponse(heartbeat=30, compress=False)
        await ws.prepare(request)

        # Replace any previous connection from this device.
        old = self.conns.get(device_id)
        if old and not old.closed:
            await old.close(code=1001, message=b"replaced")
        # Rename migration: a connection registered under the old name for the
        # same nonce belongs to this device -> close it as well.
        stale_conns = [
            old_id
            for old_id, old_nonce in self._conn_nonce.items()
            if old_id != device_id and nonce and old_nonce == nonce
        ]
        for old_id in stale_conns:
            old_conn = self.conns.pop(old_id, None)
            self._conn_nonce.pop(old_id, None)
            if old_conn and not old_conn.closed:
                await old_conn.close(code=1001, message=b"renamed")
            logger.info(f"[zkq] ws renamed: {old_id} -> {device_id}")
        self.conns[device_id] = ws
        self._conn_nonce[device_id] = nonce
        self._pending.setdefault(device_id, {})
        logger.info(f"[zkq] ws connected: {device_id}")

        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    self._dispatch_result(device_id, msg.data)
                elif msg.type == web.WSMsgType.ERROR:
                    logger.error(
                        f"[zkq] ws error from {device_id}: {ws.exception()}"
                    )
        finally:
            is_current = self.conns.get(device_id) is ws
            if is_current:
                self.conns.pop(device_id, None)
                # Only the active connection may cancel the pending queries;
                # otherwise an old connection winding down after a reconnect
                # would kill the new connection's in-flight requests.
                self._drop_pending(device_id)
            if is_current and self._conn_nonce.get(device_id) == nonce:
                self._conn_nonce.pop(device_id, None)
            logger.info(
                f"[zkq] ws disconnected: {device_id} (code={ws.close_code})"
            )
        return ws

    def _dispatch_result(self, device_id: str, raw: str) -> None:
        try:
            data = json.loads(raw)
        except Exception:
            return
        if data.get("type") == "snapshot":
            # App 通过 WS 长连接主动推送的设备快照（单一长连接通道合并）
            asyncio.get_running_loop().create_task(self._handle_ws_snapshot(device_id, data))
            return
        if data.get("type") == "qr_notify":
            # App 主动推送的扫码提取结果（提取完成后不等轮询直接推送，ADR-0003）
            asyncio.get_running_loop().create_task(self._forward_qr_notify(device_id, data))
            return
        if data.get("type") == "qr_image":
            # App 主动推送的截图（二维码/村庄图，纯推送发图，ADR-0004）：上传完立刻推帧，
            # 插件直接发图，不再等轮询。
            asyncio.get_running_loop().create_task(self._forward_qr_image(device_id, data))
            return
        request_id = data.get("requestId")
        if request_id is None:
            return
        fut = self._pending.get(device_id, {}).get(request_id)
        if fut and not fut.done():
            fut.set_result(data)

    async def _handle_ws_snapshot(self, device_id: str, data: dict) -> None:
        """处理通过 WebSocket 上报的快照帧，并回送 snapshot_ack。"""
        nonce = str(data.get("nonce") or "")
        existing = self.snapshots.get(device_id)
        if existing and existing.get("nonce") and nonce and existing["nonce"] != nonce:
            logger.warning(
                f"[zkq] ws snapshot rejected: nonce mismatch for {device_id} "
                f"have={existing.get('nonce')!r} got={nonce!r}"
            )
            return

        # Device rename migration: the same nonce under a different deviceId
        stale = [
            old_id
            for old_id, rec in self.snapshots.items()
            if old_id != device_id and nonce and rec.get("nonce") == nonce
        ]
        for old_id in stale:
            self.snapshots.pop(old_id, None)
            old_conn = self.conns.pop(old_id, None)
            self._conn_nonce.pop(old_id, None)
            if old_conn and not old_conn.closed:
                await old_conn.close(code=1001, message=b"renamed")
            logger.info(f"[zkq] renamed via ws: {old_id} -> {device_id}")

        try:
            ts = float(data.get("ts") or time.time())
        except (TypeError, ValueError):
            ts = time.time()
        snap = data.get("snapshot") or {}
        if not isinstance(snap, dict):
            snap = {}
        self.snapshots[device_id] = {
            "snapshot": snap,
            "ts": ts,
            "nonce": nonce,
        }
        await self._persist_async()
        sig = tuple(sorted(self.snapshots))
        if sig != self._tool_desc_sig:
            self._refresh_tool_descriptions()

        # 暂停超时预警跟踪（连续暂停达到 50 分钟时向管理员发送预警）
        is_running = bool(snap.get("running"))
        now = time.time()
        if not is_running:
            if device_id not in self._pause_start_times:
                self._pause_start_times[device_id] = now
            paused_duration = now - self._pause_start_times[device_id]
            if paused_duration >= 50 * 60 and not self._pause_warned.get(device_id, False):
                self._pause_warned[device_id] = True
                asyncio.get_running_loop().create_task(self._notify_pause_timeout(device_id))
        else:
            self._pause_start_times.pop(device_id, None)
            self._pause_warned.pop(device_id, None)

        # 回送 ACK 帧给设备（下发心跳频率和日志清理天数）
        conn = self.conns.get(device_id)
        if conn and not conn.closed:
            ack = {
                "type": "snapshot_ack",
                "recommended_interval": int(self.config.get("snapshot_interval", 60)),
                "log_retention_days": int(self.config.get("log_retention_days", 7)),
            }
            try:
                await conn.send_str(json.dumps(ack))
            except Exception as e:
                logger.warning(f"[zkq] failed to send snapshot_ack to {device_id}: {e}")

    async def _notify_pause_timeout(self, device_id: str) -> None:
        """设备连续暂停达到 50 分钟时，向管理员发送预警通知"""
        msg = (
            f"⚠️ 【紫孔雀脚本助手】\n\n"
            f"设备「{device_id}」已长时间暂停，辅助即将完全退出。\n\n"
            f"如需恢复挂机，请发送指令：\n"
            f"/zkq 启动 {device_id}"
        )
        logger.warning(f"[zkq] pause timeout warning for {device_id}")
        session = getattr(self, "_last_admin_session", None)
        if session:
            try:
                await self.context.send_message(session, MessageChain().message(msg))
            except Exception as e:
                logger.error(f"[zkq] send pause timeout notification failed: {e}")

    async def _forward_qr_notify(self, device_id: str, data: dict) -> None:
        """App 主动推送的通知（会话提示/会话结束）→ 转发到发起扫码提取的会话。"""
        event = self._qr_events.get(device_id)
        if event is None:
            logger.warning(f"[zkq] qr_notify dropped: no session for {device_id}")
            return
        message = str(data.get("message") or "扫码提取完成")
        try:
            await event.send(event.plain_result(message))
            logger.info(f"[zkq] qr_notify forwarded to session: {message}")
        except Exception as e:
            logger.error(f"[zkq] qr_notify forward failed: {e!r}")
        if data.get("sessionEnd"):
            # 会话结束通知：结束该设备的轮询任务（App 即将杀进程重启，ADR-0004）
            task = self._qr_tasks.get(device_id)
            if task and not task.done():
                logger.info(f"[zkq] session end notify, stopping poll for {device_id}")
                task.cancel()

    async def _forward_qr_image(self, device_id: str, data: dict) -> None:
        """App 主动推送的截图（二维码/村庄图，纯推送发图）→ 转发到发起扫码提取的会话。"""
        event = self._qr_events.get(device_id)
        if event is None:
            logger.warning(f"[zkq] qr_image dropped: no session for {device_id}")
            return
        filename = str(data.get("filename") or "")
        if not filename:
            logger.warning(f"[zkq] qr_image without filename: {data}")
            return
        caption = str(data.get("caption") or "")
        await self._qr_send_image(event, device_id, filename, caption)

    def _drop_pending(self, device_id: str) -> None:
        for fut in self._pending.get(device_id, {}).values():
            if not fut.done():
                fut.cancel()
        self._pending.pop(device_id, None)

    async def _kick_all_conns(self, message: bytes = b"token rotated") -> None:
        """Closes every live device connection (e.g. after a token rotation).

        Identity checks avoid wiping a fresh connection that reconnected while
        the old close handshakes were in flight.
        """
        targets = [(did, ws) for did, ws in list(self.conns.items()) if not ws.closed]
        await asyncio.gather(
            *(ws.close(code=1001, message=message) for _, ws in targets),
            return_exceptions=True,
        )
        for did, ws in targets:
            if self.conns.get(did) is ws:
                self.conns.pop(did, None)
                self._conn_nonce.pop(did, None)
                self._drop_pending(did)

    async def _request_query(
        self,
        device_id: str,
        query: str,
        lines: int | None = None,
        max_chars: int | None = None,
        days: int | None = None,
        hours: int | None = None,
        slot: int | None = None,
        login: str | None = None,
    ) -> tuple[dict | None, str | None]:
        """Pushes a query frame over the device's WS and awaits the result.

        @return (result_frame, None) or (None, error_message).
        """
        conn = self.conns.get(device_id)
        if not conn or conn.closed:
            logger.warning(
                f"[zkq] query '{query}' aborted: no live conn "
                f"(conn={'missing' if conn is None else 'closed'}) device={device_id}"
            )
            return None, f"设备「{device_id}」未连接，可先发 /zkq 状态 {device_id} 查看上次状态"

        self._request_counter += 1
        request_id = self._request_counter
        future = asyncio.get_running_loop().create_future()
        self._pending.setdefault(device_id, {})[request_id] = future
        frame_lines = lines if lines is not None else int(self.config.get("log_lines", 200))
        frame_max_chars = max_chars if max_chars is not None else int(self.config.get("max_chars", 4000))
        frame: dict = {
            "requestId": request_id,
            "query": query,
            "lines": frame_lines,
            "maxChars": frame_max_chars,
            "days": days or 0,
            "hours": hours or 0,
        }
        if slot is not None:
            frame["slot"] = slot
        if login is not None:
            frame["login"] = login
        try:
            await conn.send_json(frame)
            logger.info(f"[zkq] sent query '{query}' id={request_id} -> {device_id}")
        except Exception as e:
            self._pending.get(device_id, {}).pop(request_id, None)
            logger.warning(f"[zkq] query '{query}' send failed: {e!r} device={device_id}")
            return None, f"设备「{device_id}」连接异常，请稍后重试"
        try:
            data = await asyncio.wait_for(future, _QUERY_TIMEOUT)
            logger.info(f"[zkq] query '{query}' id={request_id} answered ok={data.get('ok')}")
        except asyncio.TimeoutError:
            logger.warning(f"[zkq] query '{query}' id={request_id} timed out")
            return None, f"设备「{device_id}」无响应（可能离线），请稍后重试"
        except asyncio.CancelledError:
            return None, f"设备「{device_id}」连接已断开"
        finally:
            self._pending.get(device_id, {}).pop(request_id, None)
        return data, None

    # ── Commands ────────────────────────────────────────────────────
    @filter.command("zkq", alias={"zkq帮助", "zkq 帮助", "zkq help"})
    async def zkq_cmd(self, event: AstrMessageEvent, sub: str = "", rest: GreedyStr = ""):
        """紫孔雀脚本助手管理指令（/zkq 查看完整指令菜单）"""
        sub = sub.strip()
        if not sub or sub in ("帮助", "help", "?", "？"):
            yield event.plain_result(self._help_text())
            return
        if sub in ("启动", "start", "运行", "run"):
            async for r in self.start_cmd(event, rest):
                yield r
            return
        if sub in ("停止", "stop", "暂停", "pause"):
            async for r in self.stop_cmd(event, rest):
                yield r
            return
        if sub in ("设备列表", "devices"):
            async for r in self.devices_cmd(event):
                yield r
            return
        if sub in ("全部", "all"):
            async for r in self.all_cmd(event):
                yield r
            return
        if sub in ("状态", "status"):
            async for r in self.status_cmd(event, rest):
                yield r
            return
        if sub in ("日志", "logs"):
            async for r in self.logs_cmd(event, rest):
                yield r
            return
        if sub in ("清空日志", "clearlogs"):
            async for r in self.clearlogs_cmd(event, rest):
                yield r
            return
        if sub in ("日志文件", "logfile"):
            async for r in self.logfile_cmd(event, rest):
                yield r
            return
        if sub in ("错误", "errors"):
            async for r in self.errors_cmd(event, rest):
                yield r
            return
        if sub in ("ping", "Ping"):
            async for r in self.ping_cmd(event, rest):
                yield r
            return
        if sub in ("服务器", "server", "服务器状态"):
            async for r in self.server_cmd(event):
                yield r
            return
        if sub in ("截图", "screenshot"):
            async for r in self.screenshot_cmd(event, rest):
                yield r
            return
        if sub in ("扫码提取", "qrlink", "扫码"):
            async for r in self.qr_extract_cmd(event, rest):
                yield r
            return
        if sub in ("登录方式", "choose", "选登录"):
            async for r in self.qr_choose_cmd(event, rest):
                yield r
            return
        if sub in ("确认提取", "confirm"):
            async for r in self.qr_confirm_cmd(event, rest):
                yield r
            return
        if sub in ("取消提取", "cancel"):
            async for r in self.qr_cancel_cmd(event, rest):
                yield r
            return
        if sub in ("继续提取", "qrnext", "继续"):
            async for r in self.qr_continue_cmd(event, rest):
                yield r
            return
        if sub in ("结束提取", "qrfinish", "结束"):
            async for r in self.qr_finish_cmd(event, rest):
                yield r
            return
        if sub in ("重置token", "token", "重置密钥"):
            async for r in self.token_cmd(event):
                yield r
            return
        yield event.plain_result(f"未知子命令「{sub}」，输入 /zkq 查看完整指令菜单。")

    def _help_text(self) -> str:
        return (
            "## 🤖 紫孔雀脚本助手\n\n"
            "- `/zkq 设备列表` — 已上报设备（在线状态）\n"
            "- `/zkq 状态 <设备>` — 查看状态（缺省=单设备）\n"
            "- `/zkq 启动 <设备>` — 远程启动脚本挂机（仅私聊）\n"
            "- `/zkq 停止 <设备>` — 远程停止/暂停脚本挂机（仅私聊）\n"
            "- `/zkq 全部` — 全部设备汇总\n"
            "- `/zkq 服务器` — 服务器状态（CPU/内存/磁盘）\n"
            "- `/zkq 截图 <设备>` — 设备实时截图（压缩后发图）\n"
            "- `/zkq 扫码提取 <设备> <槽位> [QQ|微信]` — 开启提取会话，远程扫码链接账号并提取存档（仅私聊）\n"
            "- `/zkq 登录方式 <设备> <QQ|微信>` — 提取流程停在登录方式选择界面时选择（未带参数会等这个，仅私聊）\n"
            "- `/zkq 确认提取 <设备>` — 村庄截图后确认并提取（仅私聊）\n"
            "- `/zkq 取消提取 <设备>` — 中止当前这一轮提取（仅私聊）\n"
            "- `/zkq 继续提取 <设备> <槽位> [QQ|微信]` — 会话里继续提下一个账号（仅私聊）\n"
            "- `/zkq 结束提取 <设备>` — 结束会话，设备自动重启恢复挂机（仅私聊）\n"
            "- `/zkq 日志 <设备>` — 最近日志（需设备在线）\n"
            "- `/zkq 日志文件 <设备>` — 完整日志打包 zip 发文件\n"
            "- `/zkq 错误 <设备>` — error.log 尾部\n"
            "- `/zkq ping <设备>` — 测试连接\n"
            "- `/zkq 重置token` — 重置设备通讯密钥（仅私聊）\n"
        )

    async def start_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """远程启动脚本挂机（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限操作。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "启动")
        if device is None:
            yield event.plain_result(hint)
            return
        data, err = await self._request_query(device, "start")
        if err:
            yield event.plain_result(err)
            return
        if not data or not data.get("ok"):
            yield event.plain_result((data or {}).get("error") or f"设备「{device}」启动失败")
            return
        msg = (data or {}).get("data") or "已启动运行"
        yield event.plain_result(f"▶️ 设备「{device}」：{msg}")

    async def stop_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """远程暂停脚本挂机（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限操作。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "停止")
        if device is None:
            yield event.plain_result(hint)
            return
        data, err = await self._request_query(device, "stop")
        if err:
            yield event.plain_result(err)
            return
        if not data or not data.get("ok"):
            yield event.plain_result((data or {}).get("error") or f"设备「{device}」停止失败")
            return
        msg = (data or {}).get("data") or "已暂停运行"
        yield event.plain_result(f"⏸️ 设备「{device}」：{msg}")

    async def devices_cmd(self, event: AstrMessageEvent):
        """查看已上报设备与在线状态"""
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        yield event.plain_result(self._format_devices())

    async def status_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """查询设备当前运行状态"""
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        device = str(rest).strip()
        yield event.plain_result(self._format_status(device))

    async def all_cmd(self, event: AstrMessageEvent):
        """查看所有设备汇总状态"""
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        yield event.plain_result(self._format_all())

    async def logs_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """查看设备最近运行日志"""
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "日志")
        if device is None:
            yield event.plain_result(hint)
            return
        yield event.plain_result(await self._query_tail(device, "logs"))

    async def clearlogs_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """清空设备历史运行日志（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "清空日志")
        if device is None:
            yield event.plain_result(hint)
            return
        data, err = await self._request_query(device, "clearlogs", days=0, max_chars=0)
        if err:
            yield event.plain_result(err)
            return
        if not data or not data.get("ok"):
            yield event.plain_result((data or {}).get("error") or "删除失败")
            return
        cleared = (data or {}).get("data", "日志已清空")
        yield event.plain_result(f"🗑️ {device} {cleared}")

    async def logfile_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """打包下载设备完整日志压缩包"""
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "日志文件")
        if device is None:
            yield event.plain_result(hint)
            return
        zip_path = await self._take_logfile(device)
        if zip_path:
            yield self._file_result(event, zip_path, f"{device}_日志_{time.strftime('%m%d_%H%M%S')}.zip")
        else:
            yield event.plain_result(
                f"📦 {device} 日志打包失败：设备离线/无日志/未响应（可先 /zkq ping {device}）"
            )

    async def _take_logfile(self, device: str) -> str | None:
        """Asks the App to zip its retained logs and upload; returns the zip path."""
        data, err = await self._request_query(device, "logfile")
        if err:
            return None
        if not data or not data.get("ok"):
            return None
        filename = str(data.get("data") or "")
        if not filename:
            return None
        # The app echoes the plugin-side filename, but a forged frame must not
        # be able to point the path outside _SCREENSHOT_DIR.
        path = _SCREENSHOT_DIR / Path(filename).name
        return str(path) if path.exists() else None

    async def errors_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """查看设备最近错误日志"""
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "错误")
        if device is None:
            yield event.plain_result(hint)
            return
        yield event.plain_result(await self._query_tail(device, "errors"))

    async def ping_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """测试与设备的网络连接"""
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "ping")
        if device is None:
            yield event.plain_result(hint)
            return
        data, err = await self._request_query(device, "ping")
        if err:
            yield event.plain_result(err)
            return
        ok = bool(data and data.get("ok"))
        result_txt = "正常" if ok else "失败"
        payload = data.get("data") if ok else (data or {}).get("error") or "无响应"
        yield event.plain_result(
            f"## 🏓 {device} 连接测试\n\n**结果**：{result_txt}\n\n> {payload}"
        )

    async def server_cmd(self, event: AstrMessageEvent):
        """查询服务器硬件运行状态"""
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        fields = await self._server_fields()
        yield event.plain_result(format_server_text(fields))

    async def screenshot_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """获取设备实时屏幕截图"""
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "截图")
        if device is None:
            yield event.plain_result(hint)
            return
        zip_path = await self._take_screenshot(device)
        if zip_path:
            yield self._file_result(event, zip_path, f"{device}_截图_{time.strftime('%m%d_%H%M%S')}.zip")
        else:
            yield event.plain_result(
                f"📷 {device} 截图失败：设备离线或没反应（可先 /zkq ping {device} 测试连接）"
            )

    def _file_result(self, event: AstrMessageEvent, path: str, name: str) -> MessageEventResult:
        return event.chain_result([File(name=name, file=path)])

    async def _take_screenshot(self, device: str) -> str | None:
        """Pushes a screenshot query over WS; returns the packed zip path or None.

        The App uploads the lossless PNG; we zip it so QQ receives the original
        file untouched (image messages get re-compressed by QQ).
        """
        data, err = await self._request_query(device, "screenshot")
        if err:
            return None
        if not data or not data.get("ok"):
            return None
        filename = str(data.get("data") or "")
        if not filename:
            return None
        # The app echoes the plugin-side filename, but a forged frame must not
        # be able to point the path outside _SCREENSHOT_DIR.
        base = Path(filename).name
        img_path = _SCREENSHOT_DIR / base
        if not img_path.exists():
            return None
        zip_path = _SCREENSHOT_DIR / (Path(base).stem + ".zip")
        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(img_path, arcname=Path(filename).name)
            return str(zip_path)
        except Exception as e:
            logger.error(f"[zkq] zip screenshot failed: {e}")
            return None

    # ── 扫码提取（ADR-0003：异步两阶段，命令即回 + 后台轮询状态机）──────
    @staticmethod
    def _parse_login(text: str) -> str | None:
        """Normalizes a login-method token: QQ/qq/微信 -> "qq", WeChat/wechat/微信 -> "wechat"."""
        low = text.lower()
        if low in ("qq", "wechat"):
            return low
        if text == "微信":
            return "wechat"
        return None

    @staticmethod
    def _login_txt(login: str | None) -> str:
        """登录方式的中文展示：qq -> QQ，wechat -> 微信，None -> 待对话选择。"""
        return "QQ" if login == "qq" else ("微信" if login == "wechat" else "（待对话选择）")

    def _parse_qr_target(self, rest: str, action: str, usage: str) -> tuple[str | None, int | None, str | None, str | None]:
        """解析 '<设备> <槽位> [QQ|微信]'，被 扫码提取 / 继续提取 共用。

        @return (device, slot, login, None) 成功；或 (None, None, None, error_hint) 失败（调用方直接回复 error_hint）。
        """
        parts = str(rest).strip().split()
        if not parts:
            return (None, None, None, usage)
        login = None
        tail = parts[-1]
        parsed_login = self._parse_login(tail)
        if parsed_login is not None:
            login = parsed_login
            parts = parts[:-1]
        if not parts:
            return (None, None, None, usage)
        slot_part = parts[-1]
        if not slot_part.isdigit() or int(slot_part) <= 0:
            return (None, None, None, f"槽位「{slot_part}」无效，请输入正整数")
        device, hint = self._resolve_device(" ".join(parts[:-1]), action)
        if device is None:
            return (None, None, None, hint)
        return (device, int(slot_part), login, None)

    async def qr_extract_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """远程扫码链接账号并提取存档（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限操作。")
            return
        device, slot, login, parse_err = self._parse_qr_target(
            str(rest), "扫码提取",
            "用法：/zkq 扫码提取 <设备> <槽位> [QQ|微信]（如：/zkq 扫码提取 手机A 1 QQ）",
        )
        if parse_err:
            yield event.plain_result(parse_err)
            return
        existing = self._qr_tasks.get(device)
        if existing and not existing.done():
            # 已有提取会话进行中：拒绝开启新会话（ADR-0004），提示用继续/结束
            yield event.plain_result(
                f"⚠️ 设备「{device}」已有提取会话进行中。\n"
                f"· /zkq 继续提取 {device} <槽位> [QQ|微信]\n"
                f"· /zkq 结束提取 {device}"
            )
            return
        data, err = await self._request_query(device, "qr_start", slot=slot, login=login)
        if err:
            yield event.plain_result(err)
            return
        if not data or not data.get("ok"):
            yield event.plain_result((data or {}).get("error") or f"设备「{device}」启动扫码提取失败")
            return
        # 异步第二段：后台轮询 qr_status，状态推进时向发起会话发图/发消息
        self._qr_events[device] = event
        self._qr_tasks[device] = asyncio.get_running_loop().create_task(
            self._qr_poll_task(device)
        )
        login_txt = self._login_txt(login)
        yield event.plain_result(
            f"✅ 已通知设备「{device}」开始扫码提取（槽位 {slot}，登录方式 {login_txt}）。二维码稍后发来。"
        )

    async def qr_choose_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """指定扫码登录方式（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限操作。")
            return
        parts = str(rest).strip().split()
        if len(parts) < 2:
            yield event.plain_result("用法：/zkq 登录方式 <设备> <QQ|微信>（如：/zkq 登录方式 手机A 微信）")
            return
        tail = parts[-1]
        login = self._parse_login(tail)
        if login is None:
            yield event.plain_result(f"登录方式「{tail}」无效，请输入 QQ 或 微信")
            return
        device, hint = self._resolve_device(" ".join(parts[:-1]), "登录方式")
        if device is None:
            yield event.plain_result(hint)
            return
        data, err = await self._request_query(device, "qr_choose", login=login)
        if err:
            yield event.plain_result(err)
            return
        if not data or not data.get("ok"):
            yield event.plain_result((data or {}).get("error") or f"设备「{device}」选择登录方式失败")
            return
        login_txt = "QQ" if login == "qq" else "微信"
        yield event.plain_result(f"🔐 已选择 {login_txt}，设备「{device}」正在继续")

    async def qr_confirm_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """村庄截图后确认并提取存档（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限操作。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "确认提取")
        if device is None:
            yield event.plain_result(hint)
            return
        data, err = await self._request_query(device, "qr_confirm")
        if err:
            yield event.plain_result(err)
            return
        if not data or not data.get("ok"):
            yield event.plain_result((data or {}).get("error") or f"设备「{device}」确认失败")
            return
        yield event.plain_result(f"✅ 已确认，设备「{device}」正在提取存档")

    async def qr_cancel_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """中止当前提取流程（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限操作。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "取消提取")
        if device is None:
            yield event.plain_result(hint)
            return
        data, err = await self._request_query(device, "qr_cancel")
        if err:
            yield event.plain_result(err)
            return
        if not data or not data.get("ok"):
            yield event.plain_result((data or {}).get("error") or f"设备「{device}」取消失败")
            return
        yield event.plain_result(f"🚫 已通知设备「{device}」取消提取")

    async def qr_continue_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """继续提取下一个账号（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限操作。")
            return
        device, slot, login, parse_err = self._parse_qr_target(
            str(rest), "继续提取",
            "用法：/zkq 继续提取 <设备> <槽位> [QQ|微信]（如：/zkq 继续提取 手机A 2 QQ）",
        )
        if parse_err:
            yield event.plain_result(parse_err)
            return
        existing = self._qr_tasks.get(device)
        if not (existing and not existing.done()):
            yield event.plain_result(f"设备「{device}」当前没有进行中的提取会话，请先 /zkq 扫码提取 开启")
            return
        data, err = await self._request_query(device, "qr_continue", slot=slot, login=login)
        if err:
            yield event.plain_result(err)
            return
        if not data or not data.get("ok"):
            yield event.plain_result((data or {}).get("error") or f"设备「{device}」继续提取失败")
            return
        login_txt = self._login_txt(login)
        yield event.plain_result(
            f"✅ 已安排设备「{device}」继续提取（槽位 {slot}，登录方式 {login_txt}）"
        )

    async def qr_finish_cmd(self, event: AstrMessageEvent, rest: GreedyStr = ""):
        """结束提取会话并恢复挂机（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限操作。")
            return
        device, hint = self._resolve_device(str(rest).strip(), "结束提取")
        if device is None:
            yield event.plain_result(hint)
            return
        existing = self._qr_tasks.get(device)
        if not (existing and not existing.done()):
            yield event.plain_result(f"设备「{device}」当前没有进行中的提取会话")
            return
        data, err = await self._request_query(device, "qr_finish")
        if err:
            yield event.plain_result(err)
            return
        if not data or not data.get("ok"):
            yield event.plain_result((data or {}).get("error") or f"设备「{device}」结束会话失败")
            return
        yield event.plain_result(f"📦 已结束设备「{device}」的提取会话，设备即将自动重启恢复挂机")

    async def _qr_poll_task(self, device: str) -> None:
        """会话模式轮询（ADR-0004）：整个提取会话期间负责发图、发对话门提示；
        会话结束（杀 App / 超时 / 中断）后退出。每轮（二维码/村庄图/对话门）标记独立，
        检测到新一轮开始（对话门 → 活跃阶段）时重置。"""
        sent_login_prompt = False
        sent_gate_prompt = False
        prev_phase = ""
        seen_active = False  # 见过非 IDLE 状态后再回 IDLE = 会话中断（如 App 重启）
        seen_gate = False  # 会话进入过对话门（AWAITING_NEXT）
        err_count = 0  # 连续查询失败次数（防网络抖动误判会话结束）
        deadline = time.time() + _QR_SESSION_DEADLINE
        task = asyncio.current_task()
        try:
            while time.time() < deadline:
                # 已被更新的会话取代（不应发生，防御）→ 安静退出：
                # 不向新会话发旧会话的消息，也不在 finally 里误删新会话的状态。
                if self._qr_tasks.get(device) is not task:
                    return
                data, err = await self._request_query(device, "qr_status")
                if err:
                    # 设备断连：若会话曾进入对话门且连续多次失败，视为会话已结束（App 被杀重启），
                    # 安静退出。单次失败（网络抖动）不结束，等 App WS 自动重连。
                    err_count += 1
                    if seen_gate and err_count >= 3 and not self.conns.get(device):
                        return
                    await asyncio.sleep(_QR_POLL_INTERVAL)
                    continue
                err_count = 0
                if not data or not data.get("ok"):
                    await asyncio.sleep(_QR_POLL_INTERVAL)
                    continue
                try:
                    payload = json.loads(str(data.get("data") or "{}"))
                except Exception:
                    payload = {}
                phase = str(payload.get("phase") or "IDLE")
                round_msg = str(payload.get("roundMessage") or "") if payload.get("roundMessage") else ""
                event = self._qr_events.get(device)

                # 新一轮开始：从对话门回到活跃阶段 → 重置每轮发送标记
                if prev_phase == "AWAITING_NEXT" and phase not in ("AWAITING_NEXT", "IDLE"):
                    sent_login_prompt = False
                    sent_gate_prompt = False
                prev_phase = phase

                if phase != "IDLE":
                    seen_active = True
                elif seen_active:
                    # 会话曾活跃又回到 IDLE：App 被重启/终止，会话已丢失
                    if self._qr_tasks.get(device) is not task:
                        return
                    if event is not None:
                        await event.send(event.plain_result(
                            f"⚠️ 设备「{device}」的提取会话已中断，设备将自行恢复。"
                        ))
                    return

                if event is not None:
                    if self._qr_tasks.get(device) is not task:
                        return
                    if phase == "AWAITING_LOGIN_CHOICE" and not sent_login_prompt:
                        sent_login_prompt = True
                        await event.send(event.plain_result(
                            f"🔐 设备「{device}」已进入登录方式选择界面，请选择：\n"
                            f"· /zkq 登录方式 {device} QQ\n"
                            f"· /zkq 登录方式 {device} 微信"
                        ))
                    if phase == "AWAITING_NEXT":
                        seen_gate = True
                        if not sent_gate_prompt:
                            sent_gate_prompt = True
                            await event.send(event.plain_result(
                                f"📋 本轮已结束：{round_msg or '完成'}。\n"
                                f"· /zkq 继续提取 {device} <槽位> [QQ|微信]\n"
                                f"· /zkq 结束提取 {device}"
                            ))
                await asyncio.sleep(_QR_POLL_INTERVAL)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[zkq] qr poll task failed: {e!r} device={device}")
        else:
            # 会话超过 _QR_SESSION_DEADLINE 上限仍未结束：告知用户已停止跟踪
            if self._qr_tasks.get(device) is task:
                event = self._qr_events.get(device)
                if event is not None:
                    try:
                        await event.send(event.plain_result(
                            f"⚠️ 设备「{device}」的提取会话已超过 {_QR_SESSION_DEADLINE // 3600} 小时，已停止跟踪。"
                        ))
                    except Exception as e:
                        logger.error(f"[zkq] qr poll timeout notify failed: {e!r}")
        finally:
            # 只清理仍属于本任务的状态，避免误删自动取消后新会话刚写入的条目
            if self._qr_tasks.get(device) is task:
                self._qr_tasks.pop(device, None)
                self._qr_events.pop(device, None)

    async def _qr_send_image(self, event: AstrMessageEvent, device: str, filename: str, caption: str) -> None:
        """Sends the QR/village screenshot as a raw PNG (QQ renders it inline
        so it can be scanned; deliberately NOT zipped like /zkq 截图)."""
        try:
            path = _SCREENSHOT_DIR / Path(filename).name
            if path.exists():
                await event.send(self._file_result(event, str(path), Path(filename).name))
            await event.send(event.plain_result(f"📷 {device}：{caption}"))
        except Exception as e:
            logger.error(f"[zkq] qr send image failed: {e!r}")

    async def token_cmd(self, event: AstrMessageEvent):
        """重置设备通讯密钥（仅私聊）"""
        if not event.is_private_chat():
            yield event.plain_result("仅私聊可用。")
            return
        if not self._check_whitelist(event):
            yield event.plain_result("无权限查询。")
            return
        tok = secrets.token_hex(16)
        self.config["token"] = tok
        try:
            await self.config.save_config_async()
        except Exception as e:
            logger.error(f"[zkq] 保存新 token 失败: {e}")
            yield event.plain_result(f"🔑 重置失败：{e}")
            return
        await self._kick_all_conns()
        logger.info("[zkq] token regenerated via command")
        yield event.plain_result(
            f"🔑 已重置设备通讯密钥：\n\n{tok}\n\n"
            "请在 App 的插件连接设置里填入该密钥。"
        )

    # ── LLM tool (全能 AI 自然语言驱动中枢) ────────────────────────
    @filter.llm_tool(name="zkq_assistant")
    async def llm_zkq_assistant(
        self,
        event: AstrMessageEvent,
        action: str,
        device: str = None,
        slot: int = None,
        login: str = None,
        param: str = None,
        lines: int = 0,
        days: int = 0,
        hours: int = 0,
    ):
        """紫孔雀脚本助手自然语言管理

        Args:
            action(string): 执行的操作类型，可选：'status'（状态查询）、'devices'（设备列表）、'start'（启动挂机）、'stop'（暂停挂机）、'screenshot'（实时截图）、'logs'（获取日志）、'clearlogs'（清空日志）、'errors'（错误日志）、'ping'（网络测试）、'server'（服务器状态）、'qr_extract'（扫码提取）、'qr_choose'（选登录方式）、'qr_confirm'（确认提取）、'qr_cancel'（取消提取）、'qr_continue'（继续提取）、'qr_finish'（结束提取）、'reset_token'（重置密钥）。
            device(string): 目标设备名称，单设备时可省略。
            slot(number): 扫码提取时的槽位编号（正整数）。
            login(string): 扫码提取登录方式，可选 'qq'（QQ）或 'wechat'（微信）。
            param(string): 附加参数或说明。
            lines(number): 获取日志最近行数。
            days(number): 获取或清理日志最近天数。
            hours(number): 获取日志最近小时数。
        """
        if not self._check_whitelist(event):
            return "无权限操作。"

        act = (action or "").strip().lower()

        # 1. 服务器硬件状态
        if act in ("server", "服务器", "服务器状态"):
            fields = await self._server_fields()
            lines_list = ["服务器状态:"]
            for key, value in fields:
                lines_list.append(f"{key}: {value}")
            return "\n".join(lines_list)

        # 2. 设备列表
        if act in ("devices", "设备列表", "设备"):
            return self._format_devices()

        # 3. 设备状态查询
        if act in ("status", "状态", "all", "全部"):
            return self._format_status((device or "").strip())

        # 4. 重置密钥
        if act in ("reset_token", "token", "重置密钥", "重置token"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            tok = secrets.token_hex(16)
            self.config["token"] = tok
            try:
                await self.config.save_config_async()
            except Exception as e:
                return f"重置失败：{e}"
            await self._kick_all_conns()
            return f"已重置设备通讯密钥：{tok}\n请在 App 的插件连接设置里填入该密钥。"

        # 下列操作均需要定位具体设备
        dev, hint = self._resolve_device((device or "").strip(), act)
        if dev is None:
            return hint

        # 5. 启动
        if act in ("start", "启动", "运行", "run"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            data, err = await self._request_query(dev, "start")
            if err:
                return err
            if not data or not data.get("ok"):
                return (data or {}).get("error") or f"设备「{dev}」启动失败"
            return f"设备「{dev}」：" + str((data or {}).get("data") or "已启动运行")

        # 6. 停止 / 暂停
        if act in ("stop", "停止", "暂停", "pause"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            data, err = await self._request_query(dev, "stop")
            if err:
                return err
            if not data or not data.get("ok"):
                return (data or {}).get("error") or f"设备「{dev}」停止失败"
            return f"设备「{dev}」：" + str((data or {}).get("data") or "已暂停运行")

        # 7. 截图
        if act in ("screenshot", "截图", "截屏"):
            path = await self._take_screenshot(dev)
            if not path:
                return f"截图失败：设备「{dev}」离线或未响应"
            await event.send(
                self._file_result(event, path, f"{dev}_截图_{time.strftime('%m%d_%H%M%S')}.zip")
            )
            return f"已发送设备「{dev}」的实时截图文件。"

        # 8. 日志压缩包
        if act in ("logs", "日志", "logfile", "日志文件"):
            n = max(0, min(int(lines or 0), 200_000))
            h = max(0, min(int(hours or 0), 24 * 30))
            d = max(0, min(int(days or 0), 365))
            data, err = await self._request_query(
                dev, "logfile", lines=n, days=d, hours=h, max_chars=0
            )
            if err:
                return err
            if not data or not data.get("ok"):
                return (data or {}).get("error") or f"设备「{dev}」打包失败"
            filename = str(data.get("data") or "")
            if not filename:
                return f"设备「{dev}」打包失败"
            path = _SCREENSHOT_DIR / Path(filename).name
            if not path.exists():
                return f"设备「{dev}」打包失败"
            scope = f"最近{d}天" if d > 0 else (f"最近{h}小时" if h > 0 else (f"最近{n}行" if n > 0 else "全部保留"))
            await event.send(
                self._file_result(event, str(path), f"{dev}_日志_{scope}_{time.strftime('%m%d_%H%M%S')}.zip")
            )
            return f"已发送日志压缩包（{scope}）"

        # 9. 清空日志
        if act in ("clearlogs", "清空日志", "删除日志"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            d = max(0, min(int(days or 0), 30))
            data, err = await self._request_query(dev, "clearlogs", days=d, max_chars=0)
            if err:
                return err
            if not data or not data.get("ok"):
                return (data or {}).get("error") or f"设备「{dev}」删除失败"
            return str(data.get("data") or "已清空日志")

        # 10. 错误日志
        if act in ("errors", "错误", "错误日志"):
            return await self._query_tail(dev, "errors")

        # 11. ping
        if act in ("ping", "Ping", "连通性", "测试"):
            data, err = await self._request_query(dev, "ping")
            if err:
                return err
            ok = bool(data and data.get("ok"))
            result_txt = "正常" if ok else "失败"
            payload = data.get("data") if ok else (data or {}).get("error") or "无响应"
            return f"设备「{dev}」连接测试：{result_txt}（{payload}）"

        # 12. 扫码提取
        if act in ("qr_extract", "扫码提取", "扫码"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            if slot is None or slot <= 0:
                return "扫码提取需要指定槽位编号（正整数），如 slot=1"
            parsed_login = self._parse_login(login or param or "")
            existing = self._qr_tasks.get(dev)
            if existing and not existing.done():
                return f"设备「{dev}」已有提取会话进行中，可告知'继续提取'或'结束提取'。"
            data, err = await self._request_query(dev, "qr_start", slot=slot, login=parsed_login)
            if err:
                return err
            if not data or not data.get("ok"):
                return (data or {}).get("error") or f"设备「{dev}」启动扫码提取失败"
            self._qr_events[dev] = event
            self._qr_tasks[dev] = asyncio.get_running_loop().create_task(
                self._qr_poll_task(dev)
            )
            login_txt = self._login_txt(parsed_login)
            return f"已通知设备「{dev}」开始扫码提取（槽位 {slot}，登录方式 {login_txt}）。二维码稍后会自动发送。"

        # 13. 选择登录方式
        if act in ("qr_choose", "登录方式", "选登录"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            parsed_login = self._parse_login(login or param or "")
            if not parsed_login:
                return "请指定登录方式（QQ 或 微信）。"
            data, err = await self._request_query(dev, "qr_choose", login=parsed_login)
            if err:
                return err
            login_txt = "QQ" if parsed_login == "qq" else "微信"
            return f"已为设备「{dev}」选择 {login_txt} 登录。"

        # 14. 确认提取
        if act in ("qr_confirm", "确认提取", "确认"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            data, err = await self._request_query(dev, "qr_confirm")
            if err:
                return err
            return f"已确认，设备「{dev}」正在提取存档。"

        # 15. 取消提取
        if act in ("qr_cancel", "取消提取", "取消"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            data, err = await self._request_query(dev, "qr_cancel")
            if err:
                return err
            return f"已通知设备「{dev}」取消提取。"

        # 16. 继续提取
        if act in ("qr_continue", "继续提取", "继续"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            slot_login = (param or "").strip().split() if param else []
            s = slot or (int(slot_login[0]) if slot_login and slot_login[0].isdigit() else None)
            if s is None or s <= 0:
                return "继续提取需要指定槽位编号，如 slot=2"
            lg = self._parse_login(login or (slot_login[1] if len(slot_login) > 1 else "") or "")
            data, err = await self._request_query(dev, "qr_continue", slot=s, login=lg)
            if err:
                return err
            login_txt = self._login_txt(lg)
            return f"已安排设备「{dev}」继续提取（槽位 {s}，登录方式 {login_txt}）。"

        # 17. 结束提取
        if act in ("qr_finish", "结束提取", "结束"):
            if not event.is_private_chat():
                return "仅私聊可用。"
            data, err = await self._request_query(dev, "qr_finish")
            if err:
                return err
            return f"已结束设备「{dev}」的提取会话，设备即将重启恢复挂机。"

        return f"未知操作指令「{act}」，可选：status / devices / start / stop / screenshot / logs / clearlogs / errors / ping / server / qr_extract 等。"

    # ── Query helpers ───────────────────────────────────────────────
    def _resolve_device(self, device: str, action: str) -> tuple[str | None, str | None]:
        """Returns (device_id, None) or (None, reply_hint).

        `action` is the command noun used in the multi-device hint (e.g. 日志/截图/ping).
        """
        if device:
            if device not in self.snapshots and device not in self.conns:
                return None, f"未找到设备「{device}」，可用 /zkq 设备列表 查看"
            return device, None
        if len(self.snapshots) == 1:
            return next(iter(self.snapshots)), None
        if not self.snapshots:
            return None, "📊 暂无设备上报（请检查 App 里的插件连接地址）"
        return None, f"多设备时请用 /zkq {action} <设备> 指定"

    async def _query_tail(self, device: str, query: str) -> str:
        rec = self.snapshots.get(device)
        data, err = await self._request_query(device, query)
        if err:
            return err
        if not data or not data.get("ok"):
            return (data or {}).get("error") or f"设备「{device}」返回错误"
        text = str(data.get("data") or "(空)")
        lines = int(self.config.get("log_lines", 50))
        age = "未知"
        if rec:
            age = fmt_age(time.time() - rec.get("ts", time.time()))
        label = "日志" if query == "logs" else "错误日志"
        return (
            f"## 📄 {device} {label}（最近 {lines} 行）\n\n"
            f"```\n{text}\n```\n\n> 更新于 {age}前"
        )

    # ── Formatting (markdown/text) ─────────────────────────────────
    def _is_online(self, device_id: str, rec: dict) -> bool:
        conn = self.conns.get(device_id)
        if conn and not conn.closed:
            return True
        interval = max(int(self.config.get("snapshot_interval", 60)), 1)
        return (time.time() - rec.get("ts", time.time())) < interval * _STALE_FACTOR

    def _format_devices(self) -> str:
        if not self.snapshots:
            return "## 📡 设备列表\n\n（暂无设备上报，请检查 App 里的插件连接地址）"
        lines = [f"## 📡 设备列表（{len(self.snapshots)} 台）"]
        now = time.time()
        for device_id, rec in sorted(self.snapshots.items()):
            age = now - rec.get("ts", now)
            online = self._is_online(device_id, rec)
            dot = "🟢" if online else "⚪"
            state = "在线" if online else "离线"
            lines.append(f"\n{dot} **{device_id}** · {state}")
            lines.append(f"> 状态 {fmt_age(age)}前更新")
        return "\n".join(lines)

    def _format_all(self) -> str:
        if not self.snapshots:
            return "## 📊 全部设备状态\n\n（暂无设备上报）"
        blocks = []
        for device_id in sorted(self.snapshots):
            blocks.append(self._format_one(device_id, self.snapshots[device_id]))
        return "\n---\n".join(blocks)

    def _format_status(self, device: str) -> str:
        if not self.snapshots:
            return "## 📊 暂无设备上报\n\n（请检查 App 里的插件连接地址）"
        if device:
            rec = self.snapshots.get(device)
            if not rec:
                return f"未找到设备「{device}」，可用 /zkq 设备列表 查看"
            return self._format_one(device, rec)
        if len(self.snapshots) == 1:
            device_id, rec = next(iter(self.snapshots.items()))
            return self._format_one(device_id, rec)
        return self._format_all() + "\n\n> 多设备请用 `/zkq 状态 <设备>` 指定"

    def _format_one(self, device_id: str, rec: dict) -> str:
        snap = rec.get("snapshot") or {}
        mode = snap.get("mode") or "未知"
        running = "是" if snap.get("running") else "否"
        acc = snap.get("currentAccount") or "-"
        remark = snap.get("currentAccountRemark") or ""
        remark_txt = f"（{remark}）" if remark else ""
        last_event = snap.get("lastEvent") or "-"
        lines = [
            f"## {device_id} 脚本状态",
            "",
            f"**模式**：{mode}",
            f"**运行**：{running}",
            f"**当前账号**：#{acc}{remark_txt}",
        ]
        # 升级完成时间只与「升级时间上号」调度相关；顺序/随机上号不显示
        if mode == "升级时间上号":
            next_txt = "-"
            next_up = snap.get("nextUpgrade")
            if isinstance(next_up, dict) and next_up.get("completionTs"):
                try:
                    remain = int(next_up["completionTs"]) - int(time.time())
                except (TypeError, ValueError):
                    remain = 0
                next_txt = f"#{next_up.get('account')} · 剩余 {fmt_duration(remain)}"
            elif next_up:
                next_txt = str(next_up)
            lines.append(f"**下次升级**：{next_txt}")
        lines.append(f"**最近事件**：{last_event}")
        age_txt = fmt_age(time.time() - rec.get("ts", time.time()))
        online = self._is_online(device_id, rec)
        dot = "🟢" if online else "⚪"
        lines[0] = f"## {dot} {device_id} 脚本状态"
        lines.append(f"> 状态 {age_txt}前更新 · 只读查询")
        return "\n".join(lines)

    # ── Server status ───────────────────────────────────────────────
    async def _server_fields(self) -> list[tuple[str, str]]:
        """Off-thread server stats so the event loop is not blocked (~0.5s)."""
        return await asyncio.to_thread(server_text)

    # ── Config / auth / whitelist / persistence ─────────────────────
    def _ensure_token(self) -> None:
        """Generates and persists a random comm token on first run.

        AstrBot's config panel has no button type, so the equivalent "reset"
        is the `/zkq 重置token` command (private chat) which regenerates it.
        """
        if str(self.config.get("token", "") or "").strip():
            return
        tok = secrets.token_hex(16)
        self.config["token"] = tok
        try:
            self.config.save_config()
        except Exception as e:
            logger.error(f"[zkq] 保存自动生成的 token 失败: {e}")
        logger.info(f"[zkq] 已自动生成设备通讯密钥，请在 App 插件连接设置里填入：{tok}")

    def _token_ok(self, request: web.Request, body_token: str | None = None) -> bool:
        """Validates the comm token (constant-time).

        Preferred channel is the X-Zkq-Token request header; query/body are
        accepted for older App builds (with a migration warning).
        """
        token = str(self.config.get("token", "") or "")
        if not token:
            return True
        provided = str(request.headers.get(_TOKEN_HEADER, "") or "")
        source = "header"
        if not provided:
            provided = str(request.query.get("token", "") or "")
            source = "query"
        if not provided and body_token is not None:
            provided = str(body_token or "")
            source = "body"
        if source != "header":
            logger.warning(
                f"[zkq] token received via {source}; upgrade App to send {_TOKEN_HEADER}"
            )
        return secrets.compare_digest(provided or "", token)

    def _refresh_tool_descriptions(self) -> None:
        """设备列表变化时刷新 LLM 工具描述，让 AI 始终知道当前有哪些设备。"""
        try:
            devices = sorted(self.snapshots)
            if not devices:
                return
            mgr = self.context.get_llm_tool_manager()
            dev_txt = "、".join(devices)
            ft = mgr.get_func("zkq_assistant")
            if ft is not None:
                ft.description = f"紫孔雀脚本助手自然语言管理。当前在线设备：{dev_txt}。"
                props = ft.parameters.get("properties") or {}
                if "device" in props:
                    props["device"]["description"] = f"目标设备名称，当前可选设备：{dev_txt}。"
            self._tool_desc_sig = tuple(devices)
            logger.info(f"[zkq] tool descriptions refreshed: {dev_txt}")
        except Exception as e:
            logger.warning(f"[zkq] refresh tool descriptions failed: {e}")

    def _check_whitelist(self, event: AstrMessageEvent) -> bool:
        """Private chats are always allowed (only the owner chats with the bot);
        the whitelist only gates group chats."""
        if event.is_private_chat():
            self._last_admin_session = str(event.unified_msg_origin)
            return True
        allowed = str(self.config.get("allowed_qq", "") or "")
        if not allowed.strip():
            self._last_admin_session = str(event.unified_msg_origin)
            return True
        sender = str(event.get_sender_id() or "")
        ok = sender in [q.strip() for q in allowed.split(",") if q.strip()]
        if ok:
            self._last_admin_session = str(event.unified_msg_origin)
        return ok

    def _load_snapshots(self) -> None:
        # 旧版本把数据写在插件目录（data/plugins/astrbot_plugin_zkq/），
        # 首次加载时若新数据目录尚无文件则从旧位置读取一次，避免已有设备列表丢失。
        legacy = _PLUGIN_DIR / "snapshots.json"
        migrated_from_legacy = False
        try:
            if _SNAPSHOT_FILE.exists():
                self.snapshots = json.loads(_SNAPSHOT_FILE.read_text(encoding="utf-8"))
            elif legacy.exists():
                self.snapshots = json.loads(legacy.read_text(encoding="utf-8"))
                migrated_from_legacy = True
                logger.info("[zkq] loaded legacy snapshots from plugin dir")
        except Exception as e:
            logger.error(f"[zkq] load snapshots failed: {e}")
            self.snapshots = {}
        self._cleanup_stale()
        # Always rewrite in sanitized form on load so legacy files that still
        # contain snapshot bodies (QQ numbers/remarks/events) are scrubbed from
        # disk immediately, even if no device reports right away.
        self._persist()
        # 迁移成功后删除旧文件：旧格式的敏感字段不应继续留在插件目录（磁盘不落盘敏感字段）。
        if migrated_from_legacy and _SNAPSHOT_FILE.exists():
            try:
                legacy.unlink(missing_ok=True)
            except OSError as e:
                logger.warning(f"[zkq] remove legacy snapshots failed: {e}")

    def _cleanup_stale(self) -> bool:
        """Drops devices with no update for _CLEANUP_DAYS (doc §14.3)."""
        cutoff = time.time() - _CLEANUP_DAYS * 86400
        stale = [
            d
            for d, rec in self.snapshots.items()
            if not isinstance(rec, dict) or rec.get("ts", 0) < cutoff
        ]
        for d in stale:
            self.snapshots.pop(d, None)
        if stale:
            logger.info(f"[zkq] cleaned {len(stale)} stale device(s)")
        return bool(stale)

    async def _cleanup_loop(self) -> None:
        """Periodically drops stale devices so they don't linger until restart."""
        while True:
            await asyncio.sleep(_CLEANUP_INTERVAL)
            try:
                if self._cleanup_stale():
                    await self._persist_async()
                    sig = tuple(sorted(self.snapshots))
                    if sig != self._tool_desc_sig:
                        self._refresh_tool_descriptions()
            except Exception as e:
                logger.error(f"[zkq] cleanup loop failed: {e}")

    def _sanitize_persisted(self) -> dict:
        """Persistable view of the snapshot table: device id, last-seen ts and
        nonce only.

        The snapshot body (QQ account numbers, remarks, last events) never
        touches disk; it lives only in memory and devices re-report it every
        ~60s, so status self-heals within a minute after a restart.
        """
        return {
            device_id: {"ts": rec.get("ts", 0), "nonce": rec.get("nonce", "")}
            for device_id, rec in self.snapshots.items()
            if isinstance(rec, dict)
        }

    def _persist(self) -> None:
        """Synchronous persist, used only on the init/load path."""
        try:
            _SNAPSHOT_FILE.write_text(
                json.dumps(self._sanitize_persisted(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.error(f"[zkq] persist snapshots failed: {e}")

    async def _persist_async(self) -> None:
        """Async persist off the event loop (snapshot hot path)."""
        async with self._persist_lock:
            try:
                payload = json.dumps(
                    self._sanitize_persisted(), ensure_ascii=False, indent=2
                )
                await asyncio.to_thread(
                    _SNAPSHOT_FILE.write_text, payload, encoding="utf-8"
                )
            except Exception as e:
                logger.error(f"[zkq] persist snapshots failed: {e}")

    async def terminate(self):
        await self._kick_all_conns(message=b"plugin stopping")
        # 取消扫码提取的后台轮询任务，避免插件重载/卸载后残留继续向旧会话发消息
        for task in list(self._qr_tasks.values()):
            task.cancel()
        self._qr_tasks.clear()
        self._qr_events.clear()
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        for task in (self._bg_task, self._cleanup_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
